package experiments.esperakka

import akka.event.ActorEventBus
import scala.beans.BeanProperty
import akka.actor.{Actor, Props, ActorSystem}
import com.gensler.scalavro.util.Union.union

case class Price(@BeanProperty symbol: String, @BeanProperty price: Double)
case class Buy(@BeanProperty symbol: String, @BeanProperty price: Double, @BeanProperty amount: Long)
case class Sell(@BeanProperty symbol: String, @BeanProperty price: Double, @BeanProperty amount: Long)


class EsperEventBusExample extends ActorEventBus with EsperClassification {

  type EsperEvents = union[Price] #or [Sell] #or [Buy]

  // you need to register all types BEFORE adding any statements or publishing any events
  registerEventType("Price", classOf[Price])
  registerEventType("Buy", classOf[Buy])
  registerEventType("Sell", classOf[Buy])


  val windowSize = 4
  val orderSize = 1000

  //
  // generate a Buy order for a quantity of orderSize at the newest price, if the simple average of the last windowSize prices is greater than the oldest price in that window
  //

  // this will delay the Price stream by windowSize - 1: the price at position latest - windowSize will fall out of the window into the Delayed stream
  epl(s"insert rstream into Delayed select rstream symbol,price from Price.std:groupwin(symbol).win:length(${windowSize-1})")
  // after every windowSize prices for a symbol, the average is inserted into the Averages stream
  epl(s"insert into Averages select symbol,avg(price) as price from Price.std:groupwin(symbol).win:length_batch($windowSize) group by symbol")
  // the join is only triggered by a new average (it has the unidrectional keyword), which (see above) is only generated after a full window for a symbol has been seen
  epl(
    s"""
      insert into Buy
      select p.symbol, p.price, $orderSize as amount
      from Price.std:unique(symbol) p
      join Delayed.std:unique(symbol) d on d.symbol = p.symbol
      join Averages a unidirectional on a.symbol = p.symbol
      where a.price > d.price
    """)

  // for debugging only
  epl("Feed", "select * from Price")
}

class BuyingActor extends Actor {
  def receive = {
    case EventBean(_,Buy(sym,price,amt)) => println(s"Buyer got a new order: $amt $sym @ $$$price")
  }
}

class Debugger extends Actor {
  def receive = {
    case EventBean(evtType,underlying) => println(s"DEBUG -  ${evtType.getName} : $underlying")
  }
}

object EsperEventBusApp extends App {
  // set up the event bus and actor(s)
  val system = ActorSystem()
  val evtBus = new EsperEventBusExample
  val buyer = system.actorOf(Props(classOf[BuyingActor]))
  val debugger = system.actorOf(Props(classOf[Debugger]))

  // subscribe to buys
  evtBus.subscribe(buyer, "inserted/Buy")
  // subscribe to various intermediate streams for debugging purposes
  evtBus.subscribe(debugger, "inserted/Feed")
  evtBus.subscribe(debugger, "inserted/Delayed")
  evtBus.subscribe(debugger, "inserted/Averages")

  val prices = Array(
    Price("BP", 7.61), Price("RDSA", 2101.00), Price("RDSA", 2209.00),
    Price("BP",7.66), Price("BP", 7.64), Price("BP", 7.67)
  )

  // feed in the market data
  prices foreach (evtBus.publishEvent(_))

  // demonstrate we can also submit Sells and Buys to the event bus, thanks to the union type
  evtBus.publishEvent(Buy("IBM",182.79, 100))
  evtBus.publishEvent(Sell("NBG",4.71, 1000))
}


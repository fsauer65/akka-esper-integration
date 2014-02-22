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

  // generate a Buy order for a quantity of 1000 at the newest price,
  // if the simple average of the last 4 prices is greater than the oldest price in that collection of 4 prices
  // This is just one way you could do this in Esper, not necessarily the best way...
  epl(
    """
      insert into Buy
      select a.symbol as symbol, d.price as price, 1000 as amount
      from pattern[every a=Price -> b=Price(symbol=a.symbol) -> c=Price(symbol=a.symbol) -> d=Price(symbol=a.symbol)]
      where (a.price + b.price + c.price + d.price) > 4*a.price
    """)
}

class BuyingActor extends Actor {
  def receive = {
    case Buy(sym,price,amt) => println(s"Got a new buy: $amt $sym @ $$$price")
  }
}

object EsperEventBusApp extends App {
  // set up the event bus and actor(s)
  val system = ActorSystem()
  val evtBus = new EsperEventBusExample
  val buyer = system.actorOf(Props(classOf[BuyingActor]))

  // subscribe to buys
  evtBus.subscribe(buyer, "inserted/Buy")

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


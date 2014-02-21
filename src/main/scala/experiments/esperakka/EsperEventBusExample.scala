package experiments.esperakka

import akka.event.ActorEventBus
import scala.beans.BeanProperty
import akka.actor.{Actor, Props, ActorSystem}
import com.gensler.scalavro.util.Union.union

sealed trait Side
case object BUY extends Side
case object SELL extends Side

case class Trade(@BeanProperty val symbol: String, @BeanProperty val price: Double, @BeanProperty side: Side)
case class Price(@BeanProperty val symbol: String, @BeanProperty val price: Double)


class EsperEventBusExample extends ActorEventBus with EsperClassification {

  type EsperEvents = union[Price] #or [Trade]

  // you need to register all types BEFORE adding any statements or publishing any events
  registerEventType("Price", classOf[Price])
  registerEventType("Trade", classOf[Trade])

  // simulate a simple trading algo: ( buy(S) if avg(S) > first(S)) in a moving window of length 4

  // delay prices by 4 events per symbol
  addStatement("insert into delayed select rstream * from Price.std:groupwin(symbol).win:length(4)")
  // running avg over the last 4 prices for each symbol
  addStatement("insert into averages select symbol, avg(price) as avgPrice from Price.win:length(4) group by symbol")
  // the actual buy rule
  addStatement(
    """
      insert into Trades
      select p.symbol as symbol, p.price as price, "BUY" as side
      from Price as p unidirectional,
           delayed.std:unique(symbol) as d,
           averages.std:unique(symbol) as a
      where p.symbol = d.symbol and p.symbol = a.symbol and a.avgPrice > d.price
    """)

}

class ConsumerActor extends Actor {
  def receive = {
    case EventBean(evtType,underlying) => println(s"Got a new event: $underlying of type ${evtType.getName}")
  }
}

object EsperEventBusApp extends App {
  // set up the event bus and actor(s)
  val system = ActorSystem()
  val evtBus = new EsperEventBusExample
  val consumer = system.actorOf(Props(classOf[ConsumerActor]))

  // subscribe to new events appearing in the MyAwesomeOutput stream
  evtBus.subscribe(consumer, "removed/delayed")
  evtBus.subscribe(consumer, "inserted/averages")
  evtBus.subscribe(consumer, "inserted/Trades")

  val marketData = Array(
    Price("BP", 7.61), Price("RDSA", 2201.00), Price("RDSA", 2209.00),
    Price("BP",7.66), Price("BP", 7.64), Price("BP", 7.67)
  )
  // feed in the market data
  marketData foreach (evtBus.publishEvent(_))
}


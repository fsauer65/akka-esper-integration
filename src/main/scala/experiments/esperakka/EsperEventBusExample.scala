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

  override def registerEsperEventTypes {
    registerEventType("Price", classOf[Price])
    registerEventType("Trade", classOf[Trade])
  }

  // add some crazy esper rules
  addStatement("insert into MarketData select * from Price where symbol='IBM' and price >= 800")
  addStatement("insert into MyTrades select * from Trade(symbol='AAPL') where price >= 800")

}

class ConsumerActor extends Actor {
  def receive = {
    case EventBean(evtType,underlying) => println(s"Got a new event: $underlying of type $evtType")
  }
}

object EsperEventBusApp extends App {
  // set up the event bus and actor(s)
  val system = ActorSystem()
  val evtBus = new EsperEventBusExample
  val consumer = system.actorOf(Props(classOf[ConsumerActor]))

  // subscribe to new events appearing in the MyAwesomeOutput stream
  evtBus.subscribe(consumer, "inserted/MarketData")
  evtBus.subscribe(consumer, "inserted/MyTrades")

  // insert a bunch of TestEvents
  val symbols = Array("AAPL", "IBM", "GOOG", "QQQ")
  val sides = Array(BUY,SELL)
  (1 to 10) foreach { i => evtBus.publishEvent(Price(symbols(i%symbols.length), i*100))}
  (1 to 10) foreach { i => evtBus.publishEvent(Trade(symbols(i%symbols.length), i*100, sides(i%2)))}
}


package experiments.streams

import akka.actor.ActorSystem
import akka.stream.FlowMaterializer
import akka.stream.MaterializerSettings
import akka.stream.scaladsl.Flow

object StreamCEP extends App {

  case class Price(symbol: String, price: Double)
  case class Buy(symbol: String, price: Double, amount: Long)
  case class Sell(symbol: String, price: Double, amount: Long)

  implicit val system = ActorSystem("Sys")
  val materializer = FlowMaterializer(MaterializerSettings())

  val prices = Array(
    Price("BP", 7.61), Price("RDSA", 2101.00), Price("RDSA", 2209.00),
    Price("BP",7.66), Price("BP", 7.64), Price("BP", 7.67)
  )

  val WINDOW = 4

  def sum(prices: Seq[Price]):Double = prices.foldLeft(0.0)((partial, price) => partial + price.price)
  def avg(prices: Seq[Price]):Double = sum(prices) / prices.size

  // just use the test data from above, could be an actual (infinite) ticker stream
  Flow(prices.toVector).
    // group by symbol, splitting the stream into separate streams, one per symbol
    groupBy(_.symbol).
    // process them all
    foreach {
       case (symbol, producer) =>
          Flow(producer).
            // chunk the stream by the window size
            grouped(WINDOW).
            // the actual rule! only let through each complete window for which avg > first
            filter(window => window.size == WINDOW && avg(window) > window.head.price).
            // for those that pass, generate a Buy
            map(window => Buy(symbol, window.reverse.head.price, 1000)).
            // send to market - for now just print them :-)
            foreach(buy => println(s"generated a buy:  $buy")).
            consume(materializer)
    }.
    onComplete(materializer) { _ => system.shutdown()}
}

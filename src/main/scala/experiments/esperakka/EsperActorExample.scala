package experiments.esperakka

import akka.actor.{Props, ActorSystem}
import experiments.esperakka.EsperActor.{StartProcessing, DeployStatement, RegisterEventType}

/**
 * Created by fsauer on 3/15/14.
 */
object EsperActorExample extends App {

  val windowSize=4
  val orderSize=1000

  val system = ActorSystem()
  val esperActor = system.actorOf(Props(classOf[EsperActor]))
  val buyer = system.actorOf(Props(classOf[BuyingActor]))
  val debugger = system.actorOf(Props(classOf[Debugger]))


  val statement1 = s"""
    insert rstream into Delayed
      select rstream symbol,price
      from Price.std:groupwin(symbol).win:length(${windowSize-1})
  """
  val statement2 = s"""
    insert into Averages
      select symbol,avg(price) as price
      from Price.std:groupwin(symbol).win:length_batch($windowSize) group by symbol
  """

  val statement3 =
    s"""
     insert into Buy
      select p.symbol, p.price, $orderSize as amount
      from Price.std:unique(symbol) p
      join Delayed.std:unique(symbol) d on d.symbol = p.symbol
      join Averages a unidirectional on a.symbol = p.symbol
      where a.price > d.price
    """


  esperActor ! RegisterEventType("Price", classOf[Price])
  esperActor ! RegisterEventType("Buy", classOf[Buy])

  esperActor ! DeployStatement(statement1 , None)
  esperActor ! DeployStatement(statement2, None)
  esperActor ! DeployStatement(statement3, Some(buyer))

  esperActor ! StartProcessing

  val prices = Array(
    Price("BP", 7.61), Price("RDSA", 2101.00), Price("RDSA", 2209.00),
    Price("BP",7.66), Price("BP", 7.64), Price("BP", 7.67)
  )

  prices foreach (esperActor ! _)
}

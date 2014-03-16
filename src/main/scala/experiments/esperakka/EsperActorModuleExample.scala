package experiments.esperakka

import akka.actor.{ActorRef, Props, ActorSystem}
import experiments.esperakka.EsperActor.{DeployModule, StartProcessing, DeployStatement, RegisterEventType}

/**
  * Created by fsauer on 3/15/14.
  */
object EsperActorModuleExample extends App {

   val windowSize=4
   val orderSize=1000

   val system = ActorSystem()
   val esperActor = system.actorOf(Props(classOf[EsperActor]))
   val buyer = system.actorOf(Props(classOf[BuyingActor]))
   val debugger = system.actorOf(Props(classOf[Debugger]))

   esperActor ! RegisterEventType("Price", classOf[Price])
   esperActor ! RegisterEventType("Buy", classOf[Buy])

   esperActor ! DeployModule(s"""
      module SimpleAverageTrader;

      @Name("Delayed")
      insert rstream into Delayed
      select rstream symbol,price
      from Price.std:groupwin(symbol).win:length(${windowSize-1});

      @Name("Averages")
      insert into Averages
      select symbol,avg(price) as price
      from Price.std:groupwin(symbol).win:length_batch($windowSize) group by symbol;

      @Name("Buy")
      insert into Buy
      select p.symbol, p.price, $orderSize as amount
      from Price.std:unique(symbol) p
      join Delayed.std:unique(symbol) d on d.symbol = p.symbol
      join Averages a unidirectional on a.symbol = p.symbol
      where a.price > d.price;
    """, Map[String,ActorRef]("Buy" -> buyer, "Delayed" -> debugger))

  esperActor ! StartProcessing

   val prices = Array(
     Price("BP", 7.61), Price("RDSA", 2101.00), Price("RDSA", 2209.00),
     Price("BP",7.66), Price("BP", 7.64), Price("BP", 7.67)
   )

   prices foreach (esperActor ! _)
 }

akka-esper-integration
======================

Example how one could integrate [Esper](http://esper.codehaus.org) with Akka by embedding an esper engine inside an Akka event bus.
Events submitted to the bus are inserted into the engine, whereas actors subscribing to the event bus
will receive the events published as a result of the esper rules firing.

# Example
    class EsperEventBusExample extends ActorEventBus with EsperClassification {

      type EsperEvents = union[Price] #or [Sell] #or [Buy]

      override def esperEventTypes = new Union[EsperEvents]

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

Note the use of union types (from [scalavro](https://github.com/GenslerAppsPod/scalavro/blob/master/util/src/main/scala/com/gensler/scalavro/util/union.scala)),
this allows us to submit events of different types into the bus without those types requiring a common super type.

The above event bus is used as follows:

    class BuyingActor extends Actor {
      def receive = {
        case Buy(sym,price,amt) => println(s"Got a new buy: $amt $sym @ $$$price")
      }
    }

    val system = ActorSystem()
    val evtBus = new EsperEventBusExample
    val buyer = system.actorOf(Props(classOf[BuyingActor]))

    // subscribe to buys
    evtBus.subscribe(buyer, "Buy")

After submitting the following data you'll see a Buy being generated:

    val prices = Array(
    Price("BP", 7.61), Price("RDSA", 2101.00), Price("RDSA", 2209.00),
    Price("BP",7.66), Price("BP", 7.64), Price("BP", 7.67)
    )

    // feed in the market data
    prices foreach (evtBus.publishEvent(_))

Output:

    Got a new buy: 1000 BP @ $7.67

Update:

##Modules

Now also supports deploying esper modules (as traits). Here is an example module:

    trait ExampleEsperModule extends EsperModule {
      self: EsperClassification =>

      val windowSize = 4
      val orderSize = 1000

      installModule(
        s"""
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
        """)
    }

And the event bus definition becomes trivial:

    class EsperEventBusWithModuleExample extends ActorEventBus with EsperClassification with ExampleEsperModule {
      type EsperEvents = union[Price] #or [Sell] #or [Buy]
      override def esperEventTypes = new Union[EsperEvents]
    }

Loading modules from external resources is also supported. You can read from a File, InputStream, URL, or a scala Source

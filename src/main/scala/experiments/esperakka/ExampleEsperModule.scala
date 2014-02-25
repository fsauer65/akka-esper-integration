package experiments.esperakka

import com.gensler.scalavro.util.Union
import com.gensler.scalavro.util.Union._

/**
 * Created by fsauer on 2/25/14.
 */
trait ExampleEsperModule {
  self: EsperClassification =>

  type EsperEvents = union[Price] #or [Sell] #or [Buy]
  override def esperEventTypes = new Union[EsperEvents]

  val windowSize = 4
  val orderSize = 1000

  val module =
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
    """

  // install the module and subscribe to the named statements listed
  installModule(module, List("Buy", "Averages", "Delayed"))
}

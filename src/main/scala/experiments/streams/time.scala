package experiments.streams

import akka.actor.ActorSystem
import akka.stream.FlowMaterializer
import akka.stream.scaladsl._
import akka.stream.stage.{PushStage, Directive, Context}
import scala.concurrent.duration._
import scala.concurrent.duration.FiniteDuration


/**
 * Stream experiments related to time
 */
object time extends App {

  implicit val system = ActorSystem("Sys")
  implicit val materializer = FlowMaterializer()

  /**
   * A PushStage that filter incoming events for a given duration and only pushes those events for
   * which the predicate remains true the entire time. Incoming events are matched with previously received
   * events using a key function E => K, with the default being the identity function. The state keeps all
   * matching events received during the interval. If a new matching event for which the predicate no longer
   * holds is received the cached events are removed and no result is pushed. If a matching events is received
   * at or after the duration the entire sequence of cached events is passed to a reduce function and the result
   * is pushed downstream. The default reducer pushes the oldest event downstream.
   *
   * @param duration    filter for this amount of time
   * @param predicate   predicate that has to hold for the entire duration
   * @param key         E=>K maps events to a key by which they are cached
   * @param reduce      Seq[(Long,E)]=>E reduces all events received during the interval to a single event to be
   *                    pushed downstream
   * @tparam E          Event type
   * @tparam K          Key type
   */
  class FilterFor[E,K](duration : FiniteDuration)(predicate: E => Boolean)
                      (key: E => K = (evt:E)=>evt.asInstanceOf[K],
                       reduce: Seq[(Long,E)] => E = (evts:Seq[(Long,E)])=>evts.head._2) extends PushStage[E,E] {

    var pending : Map[K,Seq[(Long,E)]] = Map.empty
    val nanos = duration.toNanos


    override def onPush(evt: E, ctx: Context[E]): Directive = {

      val k = key(evt)
      val now = System.nanoTime()

      pending.get(k) match {

        case Some(previous) if predicate(evt) =>
          // predicate holds for an event we already saw before
          // if elapsed time >= duration, reduce and push downstream,
          // otherwise add to cache and pull
          val withNext = previous :+ now -> evt
          if (now - previous.head._1 >= nanos) {
            pending = pending - k
            ctx.push(reduce(withNext))
          } else {
            pending = pending.updated(k, withNext)
            ctx.pull
          }

        case Some(previous) if !predicate(evt) =>
          // predicate no longer holds for pending events, remove key from cache and pull
          pending = pending - k
          ctx.pull

        case None if predicate(evt) =>
          // predicate holds for an event we have not yet seen, add to cache and pull
          pending = pending + (k -> Vector(now->evt))
          ctx.pull

        case _ =>
          // none of the above, good time to remove stale entries from the cache
          for {
            (k,(t,_)::_) <- pending // get key and oldest timestamp
            if (now - t) > nanos
          } yield {pending = pending - k}
          ctx.pull
      }
    }

  }


  // test data

  case class Tick(time:Long)

  case class Price(symbol:String, price: Double, time:Long = 0)

  def priceKey (p:Price):String = p.symbol

  val data = Source(List[Price](
      Price("IBM",155.87), Price("MSFT",47.18),
      Price("IBM",155.87), Price("MSFT",47.18),
      Price("IBM",155.87), Price("MSFT",47.18),
      Price("IBM",155.87), Price("MSFT",47.18),
      Price("IBM",155.87), Price("MSFT",47.18),
      Price("IBM",155.87), Price("MSFT",47.18),
      Price("IBM",155.87), Price("MSFT",47.18),
      Price("IBM",155.87), Price("MSFT",47.18),
      Price("IBM",155.87), Price("MSFT",47.18),
      Price("IBM",155.87), Price("MSFT",47.18),
      Price("IBM",155.87), Price("MSFT",47.18)
  ))

  // emit a tick every 100 millis
  val ticks = Source(0 second, 100 millis, () => Tick(System.nanoTime()))

  val quotes: Source[Price] = Source() { implicit b =>
     import FlowGraphImplicits._
     val out = UndefinedSink[Price]
     val zip = ZipWith[Tick,Price,Price]((t,p)=> p.copy(time = t.time))
     ticks ~> zip.left
     data ~> zip.right
     zip.out ~> out
     out
  }

  val flow = quotes.transform(() => new FilterFor[Price,String](1 seconds)(q => q.price > 150)(priceKey)).to(Sink.foreach(println(_)))
  //val flow = quotes.to(Sink.foreach(println(_)))
  flow.run()
}


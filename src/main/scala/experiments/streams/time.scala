package experiments.streams

import akka.actor.ActorSystem
import akka.stream.FlowMaterializer
import akka.stream.scaladsl.{Sink, Source}
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
          val withNext = previous :+ now -> evt
          if (now - previous.head._1 >= nanos) {
            pending = pending - k
            ctx.push(reduce(withNext))
          } else {
            pending = pending.updated(k, withNext)
            ctx.pull
          }

        case Some(previous) if !predicate(evt) =>
          pending = pending - k
          ctx.pull

        case None if predicate(evt) =>
          pending = pending + (k -> Vector(now->evt))
          ctx.pull

        case _ =>
          for {
            (k,(t,_)::_) <- pending
            if (now - t) > nanos
          } yield {pending = pending - k}
          ctx.pull
      }
    }

  }


  val ticks = Source(0 second, 100 millis, () => "Hello")

  val flow = ticks.transform(() => new FilterFor[String,String](1 seconds)(x => true)).to(Sink.foreach(println(_)))

  flow.run()
}

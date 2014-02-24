package experiments.esperakka

import com.espertech.esper.client._
import akka.event.{LookupClassification, ActorEventBus}
import com.gensler.scalavro.util.Union._

object EventBean {
  def unapply(evt: com.espertech.esper.client.EventBean) = Some(evt.getEventType, evt.getUnderlying)
}

abstract trait EsperClassification extends LookupClassification {

  this : ActorEventBus =>

  type EsperEvents

  sealed trait InternalEvent
  case class NewEvent (topic:String, evt: EventBean) extends InternalEvent
  case class RemovedEvent(topic:String, evt: EventBean) extends InternalEvent

  type Event = InternalEvent
  type Classifier = String

  val esperConfig = new Configuration()

  lazy val epService = EPServiceProviderManager.getDefaultProvider(esperConfig)
  lazy val epRuntime = epService.getEPRuntime

  protected def mapSize() = 2

  protected def registerEventType(name:String, clz: Class[_ <: Any]) {
    esperConfig.addEventType(name, clz.getName)
  }

  /**
   * The topic will be "inserted/<event-type>" or "removed/<event-type>"
   * @param event
   * @return
   */
  protected def classify(event: Event): Classifier = event match {
    case NewEvent(topic, evt) => s"inserted/$topic"
    case RemovedEvent(topic, evt) => s"removed/$topic"
  }

  // from LookupCLassification
  protected def publish(event: Event, subscriber: Subscriber): Unit = {
    event match {
      case NewEvent(_,evt) => subscriber ! evt
      case RemovedEvent(_,evt) => subscriber ! evt
    }
  }

  /**
   * @param evt this event wil be inserted into the esper runtime
   * @tparam T anything type that can be proven to be part of type EsperEvents - allows for union types
   */
  def publishEvent[T: prove[EsperEvents]#containsType](evt:T) {
    epRuntime.sendEvent(evt)
  }

  private def createEPL(epl:String, insert: EventBean=>Unit, remove: EventBean=>Unit) {
    try {
      val stat = epService.getEPAdministrator.createEPL(epl)
      stat.addListener(new UpdateListener() {
        override def update(newEvents: Array[EventBean], oldEvents: Array[EventBean]) {
          newEvents foreach (insert(_))
          // TODO see if we really need to support both cases, it would really simplify the API (and subscription topics) if we don't
          // Esper docs seem to suggest we will almost never get data in oldEvents
          // The use of rstream will deliver events leaving a window as newEvents, not oldEvents
          // Only the use of the keyword irstream will result in both newEvents and oldEvents...
          oldEvents foreach (remove(_))
        }
      })
    } catch {
      case x: EPException => println(x.getLocalizedMessage)
    }
  }

  /**
   * Create an EPL statement with the given epl.
   * Subscribers will get notified of the results by subscribing to the event type of the rule's output.
   * Most useful for 'insert into EvtType select ...' kind of rules
   * @param epl
   */
  def epl(epl: String) {
    def insert(evt: EventBean) = publish(NewEvent(evt.getEventType.getName,evt))
    def remove(evt: EventBean) = publish(RemovedEvent(evt.getEventType.getName,evt))
    createEPL(epl, insert, remove)
  }

  /**
   * Create an EPL statement with the given type and epl.
   * Subscribers will get notified of the results by subscribing to the given event type .
   * Most useful for simple 'select ...' kind of rules, where you may not know the event type of the result
   * due to projections resulting in an underlying Map
   * @param evtType event type used as the subscription topic
   * @param epl
   */
  def epl(evtType:String, epl: String) {
    def insert(evt: EventBean) = publish(NewEvent(evtType,evt))
    def remove(evt: EventBean) = publish(RemovedEvent(evtType,evt))
    createEPL(epl, insert, remove)
  }

}

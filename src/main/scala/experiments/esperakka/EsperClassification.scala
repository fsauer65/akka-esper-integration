package experiments.esperakka

import com.espertech.esper.client._
import akka.event.{LookupClassification, ActorEventBus}
import com.gensler.scalavro.util.Union._
import com.gensler.scalavro.util.Union
import scala.reflect.runtime.{currentMirror => m}

object EventBean {
  def unapply(evt: com.espertech.esper.client.EventBean) = Some(evt.getEventType, evt.getUnderlying)
}

abstract trait EsperClassification extends LookupClassification {

  this : ActorEventBus =>

  type EsperEvents <: Union.not[_]

  def esperEventTypes:Union[EsperEvents]

  case class InternalEvent (topic:String, evt: EventBean)

  type Event = InternalEvent
  type Classifier = String

  val esperConfig = new Configuration()

  // this is cool, types are now automagically registered when we need them to, but it would be even cooler
  // if we did not have to ask the application programmer to instantiate the Union... missing TYpeTags if we attempt to do it here
  esperEventTypes.typeMembers() foreach(t => registerEventType(t.typeSymbol.name.toString, m.runtimeClass(t)))

  // these now no longer have to be lazy, since types are guaranteed to be registered at this point
  val epService = EPServiceProviderManager.getDefaultProvider(esperConfig)
  val epRuntime = epService.getEPRuntime

  protected def mapSize() = 2

  protected def registerEventType(name:String, clz: Class[_ <: Any]) {
    esperConfig.addEventType(name, clz.getName)
  }

  protected def classify(event: InternalEvent): Classifier = event.topic

  // from LookupCLassification
  protected def publish(event: InternalEvent, subscriber: Subscriber): Unit = {
    subscriber ! event.evt
  }

  /**
   * @param evt this event wil be inserted into the esper runtime
   * @tparam T anything type that can be proven to be part of type EsperEvents - allows for union types
   */
  def publishEvent[T: prove[EsperEvents]#containsType](evt:T) {
    epRuntime.sendEvent(evt)
  }

  private def createEPL(epl:String)(notifySubscribers: EventBean=>Unit) {
    try {
      val stat = epService.getEPAdministrator.createEPL(epl)
      stat.addListener(new UpdateListener() {
        override def update(newEvents: Array[EventBean], oldEvents: Array[EventBean]) {
          newEvents foreach (notifySubscribers(_))
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
    createEPL(epl) {evt => publish(InternalEvent(evt.getEventType.getName,evt))}
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
    createEPL(epl) {evt => publish(InternalEvent(evtType,evt))}
  }

}

package experiments.esperakka

import com.espertech.esper.client._
import akka.event.{LookupClassification, ActorEventBus}
import com.gensler.scalavro.util.Union._
import com.gensler.scalavro.util.Union
import scala.reflect.runtime.{currentMirror => m}


abstract trait EsperClassification extends LookupClassification with EsperEngine {

  this : ActorEventBus =>

  type EsperEvents <: Union.not[_]

  def esperEventTypes:Union[EsperEvents]

  type Event = EsperEvent
  type Classifier = String

  // this is cool, types are now automagically registered when we need them to, but it would be even cooler
  // if we did not have to ask the application programmer to instantiate the Union... missing TYpeTags if we attempt to do it here
  esperEventTypes.typeMembers() foreach(t => registerEventType(t.typeSymbol.name.toString, m.runtimeClass(t)))


  protected def mapSize() = 2

  protected def classify(event: EsperEvent): Classifier = event.eventType

  // from LookupCLassification
  protected def publish(event: Event, subscriber: Subscriber): Unit = subscriber ! event

  /**
   * Create an EPL statement with the given epl.
   * Subscribers will get notified of the results by subscribing to the event type of the rule's output.
   * Most useful for 'insert into EvtType select ...' kind of rules
   * @param epl
   */
  def epl(epl: String) {
    createEPL(epl) {evt => publish(evt)}
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
    createEPL(epl) {evt => publish(EsperEvent(evtType,evt.underlying))}
  }

}

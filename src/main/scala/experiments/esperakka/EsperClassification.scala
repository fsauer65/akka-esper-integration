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
  case class NewEvent (evt: EventBean) extends InternalEvent
  case class RemovedEvent(evt: EventBean) extends InternalEvent

  type Event = InternalEvent
  type Classifier = String

  val esperConfig = new Configuration()

  lazy val epService = EPServiceProviderManager.getDefaultProvider(esperConfig)
  lazy val epRuntime = epService.getEPRuntime

  def mapSize() = 2

  protected def registerEventType(name:String, clz: Class[_ <: Any]) {
    esperConfig.addEventType(name, clz.getName)
  }

  /**
   * The topic will be "inserted/<event-type>" or "removed/<event-type>"
   * @param event
   * @return
   */
  protected def classify(event: Event): Classifier = event match {
    case NewEvent(evt) => s"inserted/${evt.getEventType.getName}"
    case RemovedEvent(evt) => s"removed/${evt.getEventType.getName}"
  }

  protected def publish(event: Event, subscriber: Subscriber): Unit = {
    event match {
      case NewEvent(evt) => subscriber ! evt
      case RemovedEvent(evt) => subscriber ! evt
    }
  }

  def publishEvent[T: prove[EsperEvents]#containsType](evt:T) {
    epRuntime.sendEvent(evt)
  }

  def addStatement(epl: String) {
    def insert(evt: EventBean) = publish(NewEvent(evt))
    def remove(evt: EventBean) = publish(RemovedEvent(evt))
    try {
      val stat = epService.getEPAdministrator.createEPL(epl)
      stat.addListener(new UpdateListener() {
        override def update(newEvents: Array[EventBean], oldEvents: Array[EventBean]) {
          newEvents foreach (insert(_))
          oldEvents foreach (remove(_))
        }
      })
    } catch {
      case x: EPException => println(x.getLocalizedMessage)
    }
  }

}

package experiments.esperakka

import com.espertech.esper.client._
import scala.util.{Failure, Success, Try}

case class EsperEvent(eventType: String, underlying: AnyRef)

trait EsperEngine {

  val esperConfig = new Configuration()

  // these are lazy so esperConfig can be configured before using it
  lazy val epService = EPServiceProviderManager.getDefaultProvider(esperConfig)
  lazy val epRuntime = epService.getEPRuntime

  def registerEventType(name:String, clz: Class[_ <: Any]) {
    esperConfig.addEventType(name, clz.getName)
  }

  def insertEvent(evt:Any) {
    epRuntime.sendEvent(evt)
  }

  def createEPL(epl:String)(notifySubscribers: EsperEvent=>Unit):Try[EPStatement] = {
    try {
      val stat = epService.getEPAdministrator.createEPL(epl)
      stat.addListener(new UpdateListener() {
        override def update(newEvents: Array[EventBean], oldEvents: Array[EventBean]) {
          newEvents foreach (evt => notifySubscribers(EsperEvent(evt.getEventType.getName, evt.getUnderlying)))
        }
      })
      Success(stat)
    } catch {
        case x: EPException => Failure(x)
    }
  }
}

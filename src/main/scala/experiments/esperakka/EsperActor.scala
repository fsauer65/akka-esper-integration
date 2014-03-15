package experiments.esperakka

import akka.actor.{ActorRef, Actor}
import com.espertech.esper.client._


object EsperActor {

  case object StartProcessing

  // register the given class with the given name with the esper engine
  case class RegisterEventType(name:String, clz: Class[_ <: Any])

  // not all statements will require result, they could simply be inserting into other streams
  case class DeployStatement(epl:String, listener: Option[ActorRef])
}

class EsperActor extends Actor with EsperEngine {
  import experiments.esperakka.EsperActor._

  override def receive = {
    case RegisterEventType(name, clz) => esperConfig.addEventType(name, clz.getName)
    case DeployStatement(epl, listener) => createEPL(epl, listener)
    case StartProcessing => context.become(dispatchingToEsper)
  }

  def dispatchingToEsper():Receive = {
    case evt@_ => epRuntime.sendEvent(evt)
  }

  private def createEPL(epl:String, listener: Option[ActorRef])  {
    try {
      val stat = epService.getEPAdministrator.createEPL(epl)
      // if we have a listener, install an update listener and forward the events to it
      listener map { l =>
        stat.addListener(new UpdateListener() {
          override def update(newEvents: Array[EventBean], oldEvents: Array[EventBean]) {
            newEvents foreach (l ! _)
          }
        })
      }

    } catch {
      case x: EPException => println(x.getLocalizedMessage)
    }
  }
}



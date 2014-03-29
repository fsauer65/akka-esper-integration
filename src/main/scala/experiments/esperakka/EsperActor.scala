package experiments.esperakka

import akka.actor.{ActorRef, Actor}

object EsperActor {

  case object StartProcessing

  // register the given class with the given name with the esper engine
  case class RegisterEventType(name:String, clz: Class[_ <: Any])

  // not all statements will require a listener, they could simply be inserting into other streams
  case class DeployStatement(epl:String, listener: Option[ActorRef])

  case class DeployModule(text: String, listeners: Map[String,ActorRef])
}

class EsperActor extends Actor with EsperEngine with EsperModule {
  import experiments.esperakka.EsperActor._

  override def receive = initializing

  private def initializing:Receive = {
    case RegisterEventType(name, clz) => esperConfig.addEventType(name, clz.getName)
    case DeployStatement(epl, listener) => createEPL(epl)(evt => listener map ( l => l ! evt))
    case DeployModule(text, listeners) => installModule(text) { evt => listeners.get(evt.eventType) map (_ ! evt)}
    case StartProcessing => context.become(dispatchingToEsper)
  }

  private def dispatchingToEsper():Receive = {
    case evt@_ => epRuntime.sendEvent(evt)
  }

}



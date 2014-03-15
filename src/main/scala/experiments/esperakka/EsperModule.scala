package experiments.esperakka

import com.espertech.esper.client.{EventBean=>EB, EPException, UpdateListener}
import scala.collection.JavaConversions._
import com.espertech.esper.client.annotation.Name
import java.net.URL
import scala.io.Source
import java.io.{InputStream, File}

trait EsperModule {
  self: EsperEngine =>

  /**
   * Install the module as an esper module and install an update listener for all statements
   * with a @Name annotation. Use the @Name value as the subscription topic
   * @param source content for the esper module to install
   */
  def installModule(source:Source)(notifySubscribers: EsperEvent=>Unit):Unit = {
    try {
      val moduleText = source.mkString
      val deploymentResult = epService.getEPAdministrator.getDeploymentAdmin.parseDeploy(moduleText)
      deploymentResult.getStatements.foreach {s =>
        // find those statements that have a @Name annotation
        s.getAnnotations.filter(a => a.annotationType == classOf[Name]).foreach {a =>
          val name = a.asInstanceOf[Name]
          // use the @Name("value") as the topic when the rule fires
          // def notifySubscribers(evt:EB) = publish(InternalEvent(name.value,evt))
          // install an UpdateListener for this rule
          s.addListener(new UpdateListener() {
            override def update(newEvents: Array[EB], oldEvents: Array[EB]) {
              newEvents foreach (evt => notifySubscribers(EsperEvent(name.value, evt.getUnderlying)))
            }
          })
        }
      }
    } catch {
      case x: EPException => println(x.getLocalizedMessage)
    } finally {
      source.close()
    }
  }

  def installModule(moduleText: String)(notifySubscribers: EsperEvent=>Unit):Unit = installModule(Source.fromString(moduleText))(notifySubscribers)

  def installModule(url:URL)(notifySubscribers: EsperEvent=>Unit):Unit = installModule(Source.fromURL(url))(notifySubscribers)

  def installModule(file: File)(notifySubscribers: EsperEvent=>Unit):Unit = installModule(Source.fromFile(file))(notifySubscribers)

  def installModule(in: InputStream)(notifySubscribers: EsperEvent=>Unit):Unit = installModule(Source.fromInputStream(in))(notifySubscribers)
}

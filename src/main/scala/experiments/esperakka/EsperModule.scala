package experiments.esperakka

import com.espertech.esper.client.{EventBean=>EB, EPException, UpdateListener}
import scala.collection.JavaConversions._
import com.espertech.esper.client.annotation.Name
import java.net.URL
import scala.io.Source
import java.io.{InputStream, File}

trait EsperModule {
  self: EsperClassification =>

  /**
   * Install the module as an esper module and install an update listener for all statements
   * with a @Name annotation. Use the @Name value as the subscription topic
   * @param source content for the esper module to install
   */
  def installModule(source:Source):Unit = {
    try {
      val moduleText = source.mkString
      val deploymentResult = epService.getEPAdministrator.getDeploymentAdmin.parseDeploy(moduleText)
      deploymentResult.getStatements.foreach {s =>
        // find those statements that have a @Name annotation
        val annotations = s.getAnnotations
        annotations.filter(a => a.annotationType == classOf[Name]).foreach {a =>
          val name = a.asInstanceOf[Name]
          // use the @Name("value") as the topic when the rule fires
          def notifySubscribers(evt:EB) = publish(InternalEvent(name.value(),evt))
          // install an UpdateListener for this rule
          s.addListener(new UpdateListener() {
            override def update(newEvents: Array[EB], oldEvents: Array[EB]) {
              newEvents foreach (notifySubscribers(_))
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

  def installModule(moduleText: String):Unit = installModule(Source.fromString(moduleText))

  def installModule(url:URL):Unit = installModule(Source.fromURL(url))

  def installModule(file: File):Unit = installModule(Source.fromFile(file))

  def installModule(in: InputStream):Unit = installModule(Source.fromInputStream(in))
}

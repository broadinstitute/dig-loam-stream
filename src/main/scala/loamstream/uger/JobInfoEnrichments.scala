package loamstream.uger

import org.ggf.drmaa.JobInfo
import loamstream.oracle.Resources.UgerResources
import scala.util.Try
import loamstream.util.Loggable

/**
 * @author clint
 * Mar 9, 2017
 */
object JobInfoEnrichments {

  final implicit class JobInfoOps(jobInfo: JobInfo) extends Loggable {
    def toUgerResources: Try[UgerResources] = {
      import scala.collection.JavaConverters._
      
      UgerResources.fromMap(jobInfo.getResourceUsage.asScala.toMap)
    }
  }
}

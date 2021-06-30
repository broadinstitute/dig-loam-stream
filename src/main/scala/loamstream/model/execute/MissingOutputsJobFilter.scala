package loamstream.model.execute

import loamstream.model.jobs.LJob
import loamstream.util.Loggable
import scala.collection.compat._

/**
 * @author clint
 * Dec 15, 2020
 */
object MissingOutputsJobFilter extends JobFilter with Loggable {
  override def shouldRun(job: LJob): Boolean = {
    val jobShouldRun = job.outputs.isEmpty || job.outputs.exists(_.isMissing)
    
    if(jobShouldRun) {
      def missingOutputs: Seq[String] = job.outputs.filter(_.isMissing).map(_.location).to(Seq).map(l => s"'${l}'")
      
      debug(s"Running job $job because these outputs are missing: ${missingOutputs.mkString(",")}")
    } else {
      debug(s"NOT running job $job because it has no missing outputs")
    }
    
    jobShouldRun
  }
}

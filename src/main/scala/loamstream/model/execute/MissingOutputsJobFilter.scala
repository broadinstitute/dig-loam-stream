package loamstream.model.execute

import loamstream.model.jobs.LJob
import loamstream.util.Loggable

/**
 * @author clint
 * Dec 15, 2020
 */
object MissingOutputsJobFilter extends JobFilter with Loggable {
  override def shouldRun(job: LJob): Boolean = {
    val result = job.outputs.isEmpty || job.outputs.exists(_.isMissing)
    
    if(!result) {
      def missingOutputs: Seq[String] = job.outputs.filter(_.isMissing).map(_.location).toSeq.map(l => s"'${l}'")
      
      debug(s"Skipping job $job because these outputs are missing: ${missingOutputs.mkString(",")}")
    }
    
    result
  }
}

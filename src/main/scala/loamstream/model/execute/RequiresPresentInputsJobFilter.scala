package loamstream.model.execute

import loamstream.model.jobs.LJob
import loamstream.util.Loggable

/**
 * @author clint
 * Nov 13, 2018
 */
object RequiresPresentInputsJobFilter extends JobFilter with Loggable {
  override def shouldRun(job: LJob): Boolean = {
    val result = job.inputs.forall(_.isPresent)

    if (!result) {
      val missingInputs = job.inputs.filter(_.isMissing)

      debug(s"${missingInputs.size} inputs are missing for job ${job}: ${missingInputs}")
    }

    result
  }
}

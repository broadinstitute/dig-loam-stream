package loamstream.model.execute

import loamstream.model.jobs.LJob
import loamstream.util.Loggable

/**
 * @author clint
 * Nov 13, 2018
 */
object RequiresPresentInputsJobCanceler extends JobCanceler with Loggable {
  override def shouldCancel(job: LJob): Boolean = {
    val result = job.inputs.exists(_.isMissing)

    if (result) {
      lazy val missingInputs = job.inputs.filter(_.isMissing)

      debug(s"Cancelling ${job} due to ${missingInputs.size} missing inputs: ${missingInputs.mkString(",")}")
    }

    result
  }
}

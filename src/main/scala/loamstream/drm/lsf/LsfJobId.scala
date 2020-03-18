package loamstream.drm.lsf

import scala.util.matching.Regex
import scala.util.Try
import loamstream.util.Tries
import loamstream.drm.DrmTaskId

/**
 * @author clint
 * May 16, 2018
 */
object LsfJobId {
  def asString(drmTaskId: DrmTaskId): String = s"${drmTaskId.jobId}[${drmTaskId.taskIndex}]"
  
  private val jobIdRegex: Regex = """(.+?)\[(\d+)\]""".r
  
  def parse(jobIdString: String): Try[DrmTaskId] = jobIdString.trim match {
    case jobIdRegex(base, index) => Try(DrmTaskId(base, index.toInt))
    case _ => Tries.failure(s"Couldn't parse LSF job id from '$jobIdString'")
  }
}

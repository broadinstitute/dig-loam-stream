package loamstream.drm.slurm

import loamstream.drm.DrmTaskId
import loamstream.util.Tries

import scala.util.Success
import scala.util.Try

/**
  * @author clint
  * @date Aug 3, 2021
  */
object SlurmDrmTaskId {
  private object Regexes {
    val jobAndTaskIndex = "^(\\d+)_(\\d+)$".r
    val jobId = "^(\\d+)$".r
    val byRange = "^(\\d+)_\\[(\\d+)-(\\d+)\\]$".r
  }
  
  /**
    * Parse a Slurm task id string into one or more DrmTaskIds.
    * 
    * Slurm's text representation of task ids, as returned by `squeue`, allows encoding multiple
    * task ids, like "foo_[1-3]", corresponding to 
    * Seq(DrmTaskId("foo", 1), DrmTaskId("foo", 2), DrmTaskId("foo", 3)), as well as single task
    * ids, like "foo_42" corresponding to DrmTaskId("foo", 42)
    *
    * @param s
    * @return
    */
  private[slurm] def parseDrmTaskIds(s: String): Try[Iterable[DrmTaskId]] = s.trim match {
    case Regexes.jobAndTaskIndex(jobId, taskIndex) => Success(Seq(DrmTaskId(jobId, taskIndex.toInt)))
    case Regexes.byRange(jobId, start, end) => Try {
      //NB: .to() makes for inclusive ranges
      (start.toInt to end.toInt).map(index => DrmTaskId(jobId, index))
    }
    //TODO: are combinations of by-range and enumerated ids allowed?
    case _ => Tries.failure(s"Couldn't parse DrmTaskId from '$s'")
  }
}

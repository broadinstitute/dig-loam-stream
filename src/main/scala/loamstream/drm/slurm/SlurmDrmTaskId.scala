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
    val byRange = "^(\\d+)_(\\d+)-(\\d+)$".r
    val enumerated = "(\\d+)_([.,]+)$".r
  }
  
  private[slurm] def parseDrmTaskIds(s: String): Try[Iterable[DrmTaskId]] = s.trim match {
    case Regexes.jobAndTaskIndex(jobId, taskIndex) => Success(Seq(DrmTaskId(jobId, taskIndex.toInt)))
    /*case Regexes.byRange(jobId, start, end) => Try {
      //TODO: .to vs .until ??
      (start.toInt to end.toInt).map(index => DrmTaskId(jobId, index))
    }
    case Regexes.enumerated(jobId, indexList) => Try {
      indexList.split("\\,").map(_.toInt).map(index => DrmTaskId(jobId, index))
    }*/
    //TODO: are combinations of by-range and enumerated ids allowed?
    //case Regexes.jobId(jobId) => Success(DrmTaskId(jobId, 0)) //TODO
    case _ => Tries.failure(s"Couldn't parse DrmTaskId from '$s'")
  }
}

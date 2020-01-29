package loamstream.drm

import loamstream.util.Terminable
import scala.util.Try

/**
 * @author clint
 * date: Jun 21, 2016
 */
trait Poller extends Terminable {
  /**
   * Synchronously inquire about the status of one or more jobs
   *
   * @param jobIds the ids of the jobs to inquire about
   * @return a map of job ids to attempts at that job's status
   */
  def poll(jobIds: Iterable[DrmTaskId]): Map[DrmTaskId, Try[DrmStatus]]
}

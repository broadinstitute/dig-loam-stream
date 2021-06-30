package loamstream.drm

import scala.util.Try

import loamstream.util.Terminable
import monix.reactive.Observable
import loamstream.model.jobs.DrmJobOracle

/**
 * @author clint
 * date: Jun 21, 2016
 */
trait Poller extends Terminable {
  /**
   * Asynchronously inquire about the status of some jobs
   *
   * @param drmTaskIds the ids of the jobs to inquire about
   * @return a map of job ids to attempts at that job's status
   */
  def poll(oracle: DrmJobOracle)(drmTaskIds: Iterable[DrmTaskId]): Observable[(DrmTaskId, Try[DrmStatus])]
}


package loamstream.drm.lsf

import loamstream.drm.Poller
import scala.util.Try
import loamstream.drm.DrmStatus

/**
 * @author clint
 * May 11, 2018
 */
final class LsfPoller extends Poller {
  /**
   * Synchronously inquire about the status of one or more jobs
   *
   * @param jobIds the ids of the jobs to inquire about
   * @return a map of job ids to attempts at that job's status
   */
  override def poll(jobIds: Iterable[String]): Map[String, Try[DrmStatus]] = {
    ???
    
    //TODO
  }
  
  override def stop(): Unit = ??? //TODO
}

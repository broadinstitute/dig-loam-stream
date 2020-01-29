package loamstream

/**
 * @author clint
 * May 25, 2018
 */
package object drm {
  /**
   * @author clint
   * date: Jun 29, 2016 
   */
  type DrmSubmissionResult = scala.util.Try[Map[DrmTaskId, DrmJobWrapper]]
  
  object DrmSubmissionResult {
    type SubmissionFailure = scala.util.Failure[Map[DrmTaskId, DrmJobWrapper]]
    val SubmissionFailure = scala.util.Failure
    
    type SubmissionSuccess = scala.util.Success[Map[DrmTaskId, DrmJobWrapper]]
    val SubmissionSuccess = scala.util.Success
  }
}

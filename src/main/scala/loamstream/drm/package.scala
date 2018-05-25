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
  type DrmSubmissionResult = scala.util.Try[Map[String, DrmJobWrapper]]
  
  object DrmSubmissionResult {
    type SubmissionFailure = scala.util.Failure[Map[String, DrmJobWrapper]]
    val SubmissionFailure = scala.util.Failure
    
    type SubmissionSuccess = scala.util.Success[Map[String, DrmJobWrapper]]
    val SubmissionSuccess = scala.util.Success
  }
}

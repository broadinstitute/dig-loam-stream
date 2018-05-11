package loamstream.drm


/**
 * @author clint
 * date: Jun 29, 2016 
 */
sealed trait DrmSubmissionResult {
  def isFailure: Boolean
}

object DrmSubmissionResult {
  final case class SubmissionFailure(cause: Exception) extends DrmSubmissionResult {
    override val isFailure: Boolean = true
  }
  
  final case class SubmissionSuccess(idsForJobs: Map[String, DrmJobWrapper]) extends DrmSubmissionResult {
    override val isFailure: Boolean = false
  }
}

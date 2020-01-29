package loamstream.drm

/**
 * @author clint
 * Jan 29, 2020
 */
final case class DrmTaskId(jobId: String, taskIndex: Int)

object DrmTaskId {
  implicit val ordering: Ordering[DrmTaskId] = new Ordering[DrmTaskId] {
    override def compare(lhs: DrmTaskId, rhs: DrmTaskId): Int = {
      lhs.jobId.compare(rhs.jobId) match {
        case 0 => lhs.taskIndex.compare(rhs.taskIndex)
        case result => result
      }
    }
  }
}

package loamstream.drm

/**
 * @author clint
 * Jan 29, 2020
 * 
 * An identifier for a task within a DRM task array.  Contains:
 * jobId: The id of the job representing the whole task array this task is a part of.
 * taskIndex: The index of this task within the task array.
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

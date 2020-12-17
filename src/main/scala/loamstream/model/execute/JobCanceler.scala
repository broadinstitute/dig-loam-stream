package loamstream.model.execute

import loamstream.model.jobs.LJob

/**
 * @author clint
 * Nov 16, 2018
 */
trait JobCanceler {
  def shouldCancel(job: LJob): Boolean
  
  final def ||(that: JobCanceler): JobCanceler = JobCanceler.Or(this, that)
}

object JobCanceler {
  object NeverCancel extends JobCanceler {
    override def shouldCancel(job: LJob): Boolean = false
  }
  
  final case class Or(lhs: JobCanceler, rhs: JobCanceler) extends JobCanceler {
    override def shouldCancel(job: LJob): Boolean = lhs.shouldCancel(job) || rhs.shouldCancel(job)
  }
}

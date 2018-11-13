package loamstream.model.execute

import loamstream.model.jobs.LJob
import loamstream.model.jobs.Execution
import loamstream.model.execute.JobFilter.AndJobFilter
import loamstream.util.Loggable

/**
 * @author clint
 * date: Aug 26, 2016
 */
trait JobFilter {
  def shouldRun(job: LJob): Boolean
  
  final def &&(other: JobFilter): JobFilter = new AndJobFilter(this, other)
}

object JobFilter {
  final class AndJobFilter(lhs: JobFilter, rhs: JobFilter) extends JobFilter {
    override def shouldRun(job: LJob): Boolean = lhs.shouldRun(job) && rhs.shouldRun(job)
  }
  
  object RunEverything extends JobFilter {
    override def shouldRun(job: LJob): Boolean = true
  }
}

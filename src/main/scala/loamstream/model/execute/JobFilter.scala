package loamstream.model.execute

import loamstream.model.execute.JobFilter.AndJobFilter
import loamstream.model.execute.JobFilter.OrJobFilter
import loamstream.model.jobs.LJob

/**
 * @author clint
 * date: Aug 26, 2016
 */
trait JobFilter {
  def shouldRun(job: LJob): Boolean
  
  final def &&(other: JobFilter): JobFilter = new AndJobFilter(this, other) //scalastyle:ignore method.name
  
  final def ||(other: JobFilter): JobFilter = new OrJobFilter(this, other)  //scalastyle:ignore method.name
}

object JobFilter {
  abstract class BooleanJobFilter(
      lhs: JobFilter, 
      rhs: JobFilter, 
      op: (Boolean, => Boolean) => Boolean) extends JobFilter {
    
    override def shouldRun(job: LJob): Boolean = op(lhs.shouldRun(job), rhs.shouldRun(job))
  }
  
  final case class AndJobFilter(lhs: JobFilter, rhs: JobFilter) extends BooleanJobFilter(lhs, rhs, _ && _)
  
  final case class OrJobFilter(lhs: JobFilter, rhs: JobFilter) extends BooleanJobFilter(lhs, rhs, _ || _)
  
  object RunEverything extends JobFilter {
    override def shouldRun(job: LJob): Boolean = true
  }
}

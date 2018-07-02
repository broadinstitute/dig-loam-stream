package loamstream.model.execute

import loamstream.model.jobs.Execution
import loamstream.model.jobs.LJob
import scala.util.matching.Regex

/**
 * @author clint
 * Jul 2, 2018
 */
object ByNameJobFilter {
  private[this] final implicit class RegexOps(val regex: Regex) extends AnyVal {
    def matches(input: String): Boolean = regex.findFirstMatchIn(input).isDefined
    
    def doesntMatch(input: String): Boolean = !matches(input)
  }
  
  def allOf(regex: Regex, rest: Regex*): JobFilter = new JobFilter {
    override def shouldRun(job: LJob): Boolean = (regex +: rest).forall(_.matches(job.name))
  }
  
  def anyOf(regex: Regex, rest: Regex*): JobFilter = new JobFilter {
    override def shouldRun(job: LJob): Boolean = (regex +: rest).exists(_.matches(job.name))
  }
  
  def noneOf(regex: Regex, rest: Regex*): JobFilter = new JobFilter {
    override def shouldRun(job: LJob): Boolean = (regex +: rest).forall(_.doesntMatch(job.name))
  }
  
  
}

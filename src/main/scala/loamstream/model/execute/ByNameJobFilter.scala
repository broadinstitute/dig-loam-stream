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
    def matches(input: String): Boolean = regex.pattern.matcher(input).matches
    
    def doesntMatch(input: String): Boolean = !matches(input)
  }
  
  def only(regex: Regex): JobFilter = new JobFilter {
    //TODO
    override def shouldRun(job: LJob): Boolean = regex.matches(job.name)
  }
  
  def except(regex: Regex): JobFilter = new JobFilter {
    //TODO
    override def shouldRun(job: LJob): Boolean = regex.doesntMatch(job.name)
  }
}

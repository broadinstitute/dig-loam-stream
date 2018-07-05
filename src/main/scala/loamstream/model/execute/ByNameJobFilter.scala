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
  
  def allOf(regex: Regex, rest: Regex*): JobFilter = allOf(regex +: rest)
  
  def allOf(regexes: Seq[Regex]): JobFilter = new AllOf(regexes)
  
  def anyOf(regex: Regex, rest: Regex*): JobFilter = anyOf(regex +: rest)
  
  def anyOf(regexes: Seq[Regex]): JobFilter = new AnyOf(regexes)
  
  def noneOf(regex: Regex, rest: Regex*): JobFilter = noneOf(regex +: rest)
  
  def noneOf(regexes: Seq[Regex]): JobFilter = new NoneOf(regexes)
  
  final class AllOf(regexes: Seq[Regex]) extends JobFilter {
    override def shouldRun(job: LJob): Boolean = regexes.forall(_.matches(job.name))
  }
  
  final class AnyOf(regexes: Seq[Regex]) extends JobFilter {
    override def shouldRun(job: LJob): Boolean = regexes.exists(_.matches(job.name))
  }
  
  final class NoneOf(regexes: Seq[Regex]) extends JobFilter {
    override def shouldRun(job: LJob): Boolean = regexes.forall(_.doesntMatch(job.name))
  }
}

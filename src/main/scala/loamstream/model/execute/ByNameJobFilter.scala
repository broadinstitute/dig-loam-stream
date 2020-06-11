package loamstream.model.execute

import loamstream.model.jobs.Execution
import loamstream.model.jobs.LJob
import scala.util.matching.Regex
import loamstream.util.Regexes

/**
 * @author clint
 * Jul 2, 2018
 */
sealed trait ByNameJobFilter extends JobFilter

object ByNameJobFilter {
  import Regexes.Implicits._
  
  def allOf(regex: Regex, rest: Regex*): ByNameJobFilter = allOf(regex +: rest)
  
  def allOf(regexes: Seq[Regex]): ByNameJobFilter = new AllOf(regexes)
  
  def anyOf(regex: Regex, rest: Regex*): ByNameJobFilter = anyOf(regex +: rest)
  
  def anyOf(regexes: Seq[Regex]): ByNameJobFilter = new AnyOf(regexes)
  
  def noneOf(regex: Regex, rest: Regex*): ByNameJobFilter = noneOf(regex +: rest)
  
  def noneOf(regexes: Seq[Regex]): ByNameJobFilter = new NoneOf(regexes)
  
  final case class AllOf(regexes: Seq[Regex]) extends ByNameJobFilter {
    override def shouldRun(job: LJob): Boolean = regexes.forall(_.matchesAnywhere(job.name))
  }
  
  final case class AnyOf(regexes: Seq[Regex]) extends ByNameJobFilter {
    override def shouldRun(job: LJob): Boolean = regexes.exists(_.matchesAnywhere(job.name))
  }
  
  final case class NoneOf(regexes: Seq[Regex]) extends ByNameJobFilter {
    override def shouldRun(job: LJob): Boolean = regexes.forall(_.doesntMatchAnywhere(job.name))
  }
}

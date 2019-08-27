package loamstream.cli

import scala.util.matching.Regex
import loamstream.model.execute.JobFilter
import loamstream.model.execute.ByNameJobFilter

/**
 * @author clint
 * Jul 2, 2018
 */
sealed trait JobFilterIntent

object JobFilterIntent {
  object AsByNameJobFilter {
    def unapply(intent: JobFilterIntent): Option[ByNameJobFilter] = intent match {
      case c: ConvertibleToByNameJobFilter => Some(c.toByNameJobFilter)
      case _ => None
    }
  }
  
  sealed trait ConvertibleToJobFilter extends JobFilterIntent {
    def toJobFilter: JobFilter
  }
  
  sealed trait ConvertibleToByNameJobFilter extends ConvertibleToJobFilter {
    final def toJobFilter: JobFilter = toByNameJobFilter
    
    def toByNameJobFilter: ByNameJobFilter
  }
  
  case object RunEverything extends ConvertibleToJobFilter {
    final override def toJobFilter: JobFilter = JobFilter.RunEverything
  }
  
  case object DontFilterByName extends JobFilterIntent
  
  final case class RunIfAllMatch(regexes: Seq[Regex]) extends ConvertibleToByNameJobFilter {
    override def toByNameJobFilter: ByNameJobFilter = ByNameJobFilter.allOf(regexes)
  }
  
  final case class RunIfAnyMatch(regexes: Seq[Regex]) extends ConvertibleToByNameJobFilter {
    override def toByNameJobFilter: ByNameJobFilter = ByNameJobFilter.anyOf(regexes)
  }
  
  final case class RunIfNoneMatch(regexes: Seq[Regex]) extends ConvertibleToByNameJobFilter {
    override def toByNameJobFilter: ByNameJobFilter = ByNameJobFilter.noneOf(regexes)
  }
}

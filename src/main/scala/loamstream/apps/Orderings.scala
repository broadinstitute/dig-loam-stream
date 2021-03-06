package loamstream.apps

import loamstream.model.jobs.Execution
import loamstream.model.jobs.LJob
import loamstream.util.TimeUtils
import java.time.LocalDateTime

/**
 * @author clint
 * Jun 4, 2019
 */
object Orderings {
  //NB: Order (LJob, Execution) tuples based on the Executions' start times (if any).
  //If no start time is present (for jobs where Resources couldn't be - or weren't -
  //determined, like Skipped jobs, those jobs/Executions come first.
  def executionTupleOrdering(a: (LJob, Execution), b: (LJob, Execution)): Boolean = {
    val (_, executionA) = a
    val (_, executionB) = b

    (executionA.resources, executionB.resources) match {
      case (Some(resourcesA), Some(resourcesB)) => lt(resourcesA.startTime, resourcesB.startTime)
      case (_, None) => false
      case _ => true
    }
  }
  
  def indexFileRowOrdering(a: IndexFiles.Row, b: IndexFiles.Row): Boolean = {
    (a.startTime, b.startTime) match {
      case (Some(startTimeA), Some(startTimeB)) => lt(startTimeA, startTimeB)
      case (_, None) => false
      case _ => true
    }
  }
  
  private def lt(a: LocalDateTime, b: LocalDateTime): Boolean = {
    import TimeUtils.toEpochMilli
    
    toEpochMilli(a) < toEpochMilli(b)
  }
}

package loamstream.apps

import loamstream.model.jobs.Execution
import loamstream.model.jobs.LJob

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
      case (Some(resourcesA), Some(resourcesB)) => {
        resourcesA.startTime.toEpochMilli < resourcesB.startTime.toEpochMilli
      }
      case (_, None) => false
      case _ => true
    }
  }
}

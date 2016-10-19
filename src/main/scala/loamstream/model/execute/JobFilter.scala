package loamstream.model.execute

import loamstream.model.jobs.LJob
import loamstream.model.jobs.Output
import loamstream.db.LoamDao
import loamstream.util.Loggable
import java.nio.file.Path
import loamstream.util.ValueBox
import loamstream.model.jobs.Output.CachedOutput
import loamstream.model.jobs.Output.PathOutput
import loamstream.util.TimeEnrichments
import loamstream.util.Traversables
import loamstream.model.jobs.Execution
import loamstream.model.jobs.JobState.CommandResult

/**
 * @author clint
 * date: Aug 26, 2016
 */
trait JobFilter {
  def shouldRun(job: LJob): Boolean

  def record(executions: Iterable[Execution]): Unit
}

object JobFilter {
  object RunEverything extends JobFilter {
    override def shouldRun(job: LJob): Boolean = true

    override def record(executions: Iterable[Execution]): Unit = ()
  }
}
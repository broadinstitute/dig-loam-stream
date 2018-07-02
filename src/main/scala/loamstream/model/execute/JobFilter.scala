package loamstream.model.execute

import loamstream.model.jobs.LJob
import loamstream.model.jobs.Execution

/**
 * @author clint
 * date: Aug 26, 2016
 */
trait JobFilter {
  def shouldRun(job: LJob): Boolean
}

object JobFilter {
  object RunEverything extends JobFilter {
    override def shouldRun(job: LJob): Boolean = true
  }
}

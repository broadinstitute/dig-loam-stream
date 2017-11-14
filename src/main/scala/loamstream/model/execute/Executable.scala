package loamstream.model.execute

import loamstream.model.jobs.JobNode
import loamstream.model.jobs.LJob
import loamstream.model.jobs.NoOpJob

/** A container of jobs to be executed */
final case class Executable(jobNodes: Set[JobNode]) {
  def jobs: Set[LJob] = jobNodes.map(_.job)
  
  /** Returns a new executable containing the jobs of this and that executable */
  def ++(oExecutable: Executable): Executable = Executable(jobs ++ oExecutable.jobs)

  /** Returns a new executable with a no-op root job added */
  def plusNoOpRootJob: Executable = Executable(Set(NoOpJob(jobNodes)))
  
  def plusNoOpRootJobIfNeeded: Executable = {
    if(jobNodes.size > 1) { Executable(Set(NoOpJob(jobNodes))) }
    else { this }
  }
}

/** A container of jobs to be executed */
object Executable {
  /** An empty executable with no jobs */
  val empty: Executable = Executable(Set.empty)
}

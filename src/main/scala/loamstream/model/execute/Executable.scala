package loamstream.model.execute

import loamstream.model.jobs.{LJob, NoOpJob}

/** A container of jobs to be executed */
final case class Executable(jobs: Set[LJob]) {
  /** Returns a new executable containing the jobs of this and that executable */
  def ++(oExecutable: Executable): Executable = Executable(jobs ++ oExecutable.jobs)

  /** Returns a new executable with a no-op root job added */
  def plusNoOpRootJob: Executable = Executable(Set(NoOpJob(jobs)))
  
  def plusNoOpRootJobIfNeeded: Executable = {
    if(jobs.size > 1) { Executable(Set(NoOpJob(jobs))) }
    else { this }
  }
}

/** A container of jobs to be executed */
object Executable {
  /** An empty executable with no jobs */
  val empty = Executable(Set.empty)
}
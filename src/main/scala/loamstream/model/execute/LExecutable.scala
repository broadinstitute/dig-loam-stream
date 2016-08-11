package loamstream.model.execute

import loamstream.model.jobs.{LJob, NoOpJob}

/** A container of jobs to be executed */
final case class LExecutable(jobs: Set[LJob]) {
  /** Returns a new executable containing the jobs of this and that executable */
  def ++(oExecutable: LExecutable): LExecutable = LExecutable(jobs ++ oExecutable.jobs)

  /** Returns a new executable with a no-op root job added */
  def plusNoOpRootJob: LExecutable = LExecutable(Set(NoOpJob(jobs)))
}

/** A container of jobs to be executed */
object LExecutable {
  /** An empty executable with no jobs */
  val empty = LExecutable(Set.empty)
}
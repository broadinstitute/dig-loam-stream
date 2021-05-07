package loamstream.model.execute

import loamstream.model.jobs.JobNode
import loamstream.model.jobs.LJob
import loamstream.util.Observables
import monix.reactive.Observable

/** A container of jobs to be executed */
final case class Executable(jobNodes: Set[JobNode]) {
  def jobs: Set[LJob] = jobNodes.map(_.job)
  
  /** Returns a new executable containing the jobs of this and that executable */
  def ++(oExecutable: Executable): Executable = Executable(jobs ++ oExecutable.jobs)

  def multiplex[A](f: JobNode => Observable[A]): Observable[A] = Observables.merge(jobNodes.map(f))
  
  def allJobs: Iterable[LJob] = DryRunner.toBeRun(JobFilter.RunEverything, this)
}

/** A container of jobs to be executed */
object Executable {
  /** An empty executable with no jobs */
  val empty: Executable = Executable(Set.empty)
}

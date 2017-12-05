package loamstream.model.execute

import loamstream.model.jobs.JobNode
import loamstream.model.jobs.LJob
import loamstream.model.jobs.NoOpJob
import loamstream.util.Observables
import rx.lang.scala.Observable

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
  
  def withoutNoOpJobNode: Executable = {
    def firstJobNode = jobNodes.head
    
    val noOpRoot = jobNodes.size == 1 && firstJobNode.job.isInstanceOf[NoOpJob]
    
    if(noOpRoot) { copy(jobNodes = firstJobNode.inputs) }
    else { this }
  }
  
  def multiplex[A](f: JobNode => Observable[A]): Observable[A] = Observables.merge(jobNodes.map(f))
}

/** A container of jobs to be executed */
object Executable {
  /** An empty executable with no jobs */
  val empty: Executable = Executable(Set.empty)
}

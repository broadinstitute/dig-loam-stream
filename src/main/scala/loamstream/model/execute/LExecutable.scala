package loamstream.model.execute

import loamstream.model.jobs.{LJob, NoOpJob}

/**
  * RugLoom - A prototype for a pipeline building toolkit
  * Created by oruebenacker on 2/24/16.
  */
final case class LExecutable(jobs: Set[LJob]) {
  def ++(oExecutable: LExecutable): LExecutable = LExecutable(jobs ++ oExecutable.jobs)

  def addNoOpRootJob: LExecutable = LExecutable(Set(new NoOpJob(jobs)))
}

object LExecutable {
  val empty = LExecutable(Set.empty)
}
package loamstream.model.execute

import loamstream.model.jobs.LJob
import scala.concurrent.Future
import loamstream.model.jobs.LJob.Result
import scala.concurrent.ExecutionContext

/**
  * RugLoom - A prototype for a pipeline building toolkit
  * Created by oruebenacker on 2/24/16.
  */
final case class LExecutable(jobs: Set[LJob]) {
  def ++(oExecutable: LExecutable): LExecutable = LExecutable(jobs ++ oExecutable.jobs)
}

object LExecutable {
  val empty = LExecutable(Set.empty)
}
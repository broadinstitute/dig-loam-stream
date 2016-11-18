package loamstream.model.jobs.ops

import loamstream.loam.ops.filters.LoamStoreFilter
import loamstream.model.jobs.{JobState, LJob, Output}

import scala.concurrent.{ExecutionContext, Future}

/** Job which creates a new store by filtering an existing store */
final case class StoreFilterJob[Store](input: LJob, output: Output, filter: LoamStoreFilter[Store]) extends LJob {
  /** Any jobs this job depends on */
  override def inputs: Set[LJob] = Set(input)

  /** Any outputs produced by this job */
  override def outputs: Set[Output] = Set(output)

  /**
    * Implementions of this method will do any actual work to be performed by this job
    */
  override protected def executeSelf(implicit context: ExecutionContext): Future[JobState] = ???

  override protected def doWithInputs(newInputs: Set[LJob]): LJob = ???
}

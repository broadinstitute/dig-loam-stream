package loamstream.model.jobs.ops

import java.nio.file.Path

import loamstream.loam.ops.TextStoreRecord
import loamstream.loam.ops.filters.LoamStoreFilter
import loamstream.model.jobs.ops.StoreFilterJob.LineFilter
import loamstream.model.jobs.{JobResult, LJob, Output}
import loamstream.util.Files

import scala.concurrent.{ExecutionContext, Future}
import scala.reflect.runtime.universe.Type
import loamstream.model.execute.ExecutionEnvironment

/** Job which creates a new store by filtering an existing store */
object StoreFilterJob {

  final case class LineFilter(filter: LoamStoreFilter.Untyped, tpe: Type) extends (String => Boolean) {
    override def apply(line: String): Boolean = {
      val record = TextStoreRecord(line)
      
      filter.testDynamicallyTyped(record, tpe)
    }
  }

}

/** Job which creates a new store by filtering an existing store */
final case class StoreFilterJob(
    inPath: Path, 
    outPath: Path, 
    inType: Type, 
    inputs: Set[LJob], 
    outputs: Set[Output],
    filter: LoamStoreFilter.Untyped) extends LJob {
  
  //TODO: See if this is always the case
  override def executionEnvironment: ExecutionEnvironment = ExecutionEnvironment.Local
  
  override protected def doWithInputs(newInputs: Set[LJob]): LJob = copy(inputs = newInputs)

  /** Implementations of this method will do any actual work to be performed by this job */
  override protected def executeSelf(implicit context: ExecutionContext): Future[JobResult] = Future {
    Files.filterFile(inPath, outPath)(LineFilter(filter, inType))
    JobResult.Succeeded
  }
}

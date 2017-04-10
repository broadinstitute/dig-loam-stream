package loamstream.model.jobs.ops

import java.nio.file.Path

import loamstream.loam.ops.TextStoreRecord
import loamstream.loam.ops.filters.LoamStoreFilter
import loamstream.model.execute.Resources.LocalResources
import loamstream.model.jobs.ops.StoreFilterJob.LineFilter
import loamstream.model.jobs.{Execution, JobResult, JobStatus, LJob, Output}
import loamstream.util.{Files, Futures, TimeUtils}

import scala.concurrent.{ExecutionContext, Future}
import scala.reflect.runtime.universe.Type
import loamstream.model.execute.{ExecutionEnvironment, LocalSettings}
import loamstream.model.jobs.JobResult.CommandInvocationFailure

import scala.util.{Failure, Success}

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
final case class StoreFilterJob(inPath: Path,
                                outPath: Path,
                                inType: Type,
                                inputs: Set[LJob],
                                outputs: Set[Output],
                                filter: LoamStoreFilter.Untyped) extends LJob {

  //TODO: See if this is always the case
  override def executionEnvironment: ExecutionEnvironment = ExecutionEnvironment.Local

  override protected def doWithInputs(newInputs: Set[LJob]): LJob = copy(inputs = newInputs)

  /** Implementations of this method will do any actual work to be performed by this job */
  override protected def executeSelf(implicit context: ExecutionContext): Future[Execution] = {
    Futures.runBlocking {
      val (exitValueAttempt, (start, end)) = TimeUtils.startAndEndTime {
        trace(s"RUNNING: StoreFilterJob")
        Files.filterFile(inPath, outPath)(LineFilter(filter, inType))
      }

      val resources = LocalResources(start, end)

      val (jobStatus, jobResultOpt) = exitValueAttempt match {
        case Success(_) => (JobStatus.Succeeded, Option(JobResult.Success))
        case Failure(e) => (JobStatus.FailedWithException, Option(CommandInvocationFailure(e)))
      }

      Execution(id = None,
                executionEnvironment,
                cmd = None,
                LocalSettings(),
                jobStatus,
                jobResultOpt,
                Option(resources),
                outputs.map(_.toOutputRecord))
    }
  }
}

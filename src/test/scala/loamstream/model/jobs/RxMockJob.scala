package loamstream.model.jobs

import java.time.Instant

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import loamstream.TestHelpers
import loamstream.model.execute.LocalSettings
import loamstream.util.Futures
import loamstream.util.Observables
import loamstream.util.ValueBox
import loamstream.model.execute.Settings


/**
 * @author kaan
 * @author clint
 *         date: Sep 15, 2016
 */
final case class RxMockJob( 
  override val name: String,
  override val dependencies: Set[JobNode],
  inputs: Set[DataHandle],
  outputs: Set[DataHandle],
  runsAfter: Option[RxMockJob],
  toReturnFn: RxMockJob => RunData)(implicit executions: ValueBox[Vector[RxMockJob]]) extends LocalJob {

  def toReturn: RunData = toReturnFn(this)
  
  override def initialSettings: Settings = LocalSettings

  private[this] val count = ValueBox(0)

  def executionCount = count.value

  private[this] val lastRunTimeRef: ValueBox[Option[Instant]] = ValueBox(None)
  
  def lastRunTime: Option[Instant] = lastRunTimeRef()
  
  private def waitIfNecessary(): Unit = {
    runsAfter.foreach { jobToWaitFor =>
      import loamstream.util.Observables.Implicits._
      
      val finalDepState = jobToWaitFor.lastStatus
      
      loamstream.TestHelpers.waitFor(finalDepState.firstAsFuture)
    }
  }

  override def execute(implicit context: ExecutionContext): Future[RunData] = {

    executions.mutate { oldExecutions =>
      
      val newExecutions = oldExecutions :+ this

      // :(
      count.mutate(_ + 1)  
      
      newExecutions
    }
    
    Future(waitIfNecessary()).map { _ => 
    
      trace(s"Starting job: $name")

      lastRunTimeRef := Some(Instant.now)
      
      trace(s"Finishing job: $name")

      toReturn
    }
  }

  override def toString: String = name
}

object RxMockJob {
  def apply(name: String,
            dependencies: Set[JobNode] = Set.empty,
            inputs: Set[DataHandle] = Set.empty,
            outputs: Set[DataHandle] = Set.empty,
            runsAfter: Option[RxMockJob] = None,
            toReturn: () => JobResult = () => JobResult.CommandResult(0))
            (implicit 
                  executions: ValueBox[Vector[RxMockJob]] = ValueBox(Vector.empty), 
                  discriminator: Int = 42): RxMockJob = {

    RxMockJob(name,
              dependencies,
              inputs,
              outputs,
              runsAfter,
              job => runDataFrom(job, outputs, jobResult = toReturn()))
  }

  private[this] def runDataFrom(job: LJob, outputs: Set[DataHandle], jobResult: JobResult): RunData = {
    RunData(
        job = job,
        settings = LocalSettings,
        jobStatus = jobResult.toJobStatus,
        jobResult = Option(jobResult),
        resourcesOpt = None,
        jobDirOpt = None,
        terminationReasonOpt = None)
  }
}

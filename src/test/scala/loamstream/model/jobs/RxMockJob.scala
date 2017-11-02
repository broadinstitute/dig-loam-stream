package loamstream.model.jobs

import loamstream.TestHelpers

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import loamstream.model.execute.{ExecutionEnvironment, LocalSettings}
import loamstream.util.Futures
import loamstream.util.Observables
import loamstream.util.ValueBox
import java.time.Instant


/**
 * @author kaan
 * @author clint
 *         date: Sep 15, 2016
 */
// scalastyle:off magic.number
final case class RxMockJob( 
  override val name: String,
  inputs: Set[LJob],
  outputs: Set[Output],
  runsAfter: Option[RxMockJob],
  toReturn: () => Execution)(implicit executions: ValueBox[Vector[RxMockJob]]) extends LJob {

  override def executionEnvironment: ExecutionEnvironment = TestHelpers.env

  private[this] val count = ValueBox(0)

  def executionCount = count.value

  private[this] val lastRunTimeRef: ValueBox[Option[Instant]] = ValueBox(None)
  
  def lastRunTime: Option[Instant] = lastRunTimeRef()
  
  private def waitIfNecessary(): Unit = {
    runsAfter.foreach { jobToWaitFor =>
      import loamstream.util.ObservableEnrichments._
      
      val finalDepState = jobToWaitFor.lastStatus
      
      Futures.waitFor(finalDepState.firstAsFuture)
    }
  }

  override def execute(implicit context: ExecutionContext): Future[Execution] = {

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

      toReturn()
    }
  }

  override protected def doWithInputs(newInputs: Set[LJob]): LJob = copy(inputs = newInputs)

  override def toString: String = name
}

object RxMockJob {
  def apply(name: String,
            inputs: Set[LJob] = Set.empty,
            outputs: Set[Output] = Set.empty,
            runsAfter: Option[RxMockJob] = None,
            toReturn: () => JobResult = () => JobResult.CommandResult(0))
            (implicit 
                  executions: ValueBox[Vector[RxMockJob]] = ValueBox(Vector.empty), 
                  discriminator: Int = 42): RxMockJob = {

    RxMockJob(name,
              inputs,
              outputs,
              runsAfter,
              () => executionFrom(outputs, jobResult = toReturn()))
  }

  private[this] def executionFrom(outputs: Set[Output], jobResult: JobResult) = {
    Execution(id = None,
              TestHelpers.env,
              cmd = None,
              settings = LocalSettings(),
              jobResult.toJobStatus,
              Option(jobResult),
              resources = None,
              outputs.map(_.toOutputRecord))
  }
}
// scalastyle:on magic.number

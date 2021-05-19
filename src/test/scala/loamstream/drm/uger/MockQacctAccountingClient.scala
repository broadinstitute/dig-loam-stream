package loamstream.drm.uger

import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.util.Try

import loamstream.conf.UgerConfig
import loamstream.drm.AccountingClient
import loamstream.drm.DrmTaskId
import loamstream.model.execute.Resources.DrmResources
import loamstream.model.jobs.TerminationReason
import loamstream.util.CommandInvoker
import loamstream.util.RunResults
import loamstream.util.ValueBox
import loamstream.util.LogContext
import scala.concurrent.duration.FiniteDuration
import monix.execution.Scheduler
import monix.eval.Task

/**
 * @author clint
 * Mar 15, 2017
 */
final class MockQacctAccountingClient(
    delegateFn: DrmTaskId => Try[RunResults],
    ugerConfig: UgerConfig = UgerConfig(),
    delayStart: FiniteDuration = CommandInvoker.Async.Retrying.defaultDelayStart,
    delayCap: FiniteDuration = CommandInvoker.Async.Retrying.defaultDelayCap) extends AccountingClient {
  
  private val timesGetQacctOutputForInvokedBox: ValueBox[Int] = ValueBox(0)

  private val timesGetResourceUsageInvokedBox: ValueBox[Int] = ValueBox(0)
  
  private val timesGetTerminationReasonInvokedBox: ValueBox[Int] = ValueBox(0)
  
  def timesGetQacctOutputForInvoked: Int = timesGetQacctOutputForInvokedBox()
    
  def timesGetResourceUsageInvoked: Int = timesGetResourceUsageInvokedBox()
  
  def timesGetTerminationReasonInvoked: Int = timesGetTerminationReasonInvokedBox()

  private val actualDelegate = {
    val fakeBinaryName = "MOCK"
    
    val wrappedDelegateFn: DrmTaskId => Try[RunResults] = { taskId =>
      timesGetQacctOutputForInvokedBox.mutate(_ + 1)

      delegateFn(taskId)
    }

    import scala.concurrent.ExecutionContext.Implicits.global
    
    val invoker = {
      import LogContext.Implicits.Noop
      
      CommandInvoker.Async.Retrying[DrmTaskId](
          ugerConfig.maxRetries, 
          "MOCK", 
          wrappedDelegateFn, 
          delayStart, 
          delayCap,
          scheduler = Scheduler.computation())
    }
    
    new QacctAccountingClient(invoker)
  }

  override def getResourceUsage(taskId: DrmTaskId): Task[DrmResources] = {
    timesGetResourceUsageInvokedBox.mutate(_ + 1)
    
    actualDelegate.getResourceUsage(taskId)
  }
  
  override def getTerminationReason(taskId: DrmTaskId): Task[Option[TerminationReason]] = {
    timesGetTerminationReasonInvokedBox.mutate(_ + 1)
    
    actualDelegate.getTerminationReason(taskId)
  }
}

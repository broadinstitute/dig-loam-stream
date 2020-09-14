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
import rx.lang.scala.schedulers.ComputationScheduler

/**
 * @author clint
 * Mar 15, 2017
 */
final class MockQacctAccountingClient(
    delegateFn: DrmTaskId => Try[RunResults],
    ugerConfig: UgerConfig = UgerConfig(),
    delayStart: Duration = CommandInvoker.Async.Retrying.defaultDelayStart,
    delayCap: Duration = CommandInvoker.Async.Retrying.defaultDelayCap) extends AccountingClient {
  
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
      CommandInvoker.Async.Retrying[DrmTaskId](
          ugerConfig.maxRetries, 
          "MOCK", 
          wrappedDelegateFn, 
          delayStart, 
          delayCap,
          scheduler = ComputationScheduler())
    }
    
    new QacctAccountingClient(invoker)
  }

  override def getResourceUsage(taskId: DrmTaskId): Future[DrmResources] = {
    timesGetResourceUsageInvokedBox.mutate(_ + 1)
    
    actualDelegate.getResourceUsage(taskId)
  }
  
  override def getTerminationReason(taskId: DrmTaskId): Future[Option[TerminationReason]] = {
    timesGetTerminationReasonInvokedBox.mutate(_ + 1)
    
    actualDelegate.getTerminationReason(taskId)
  }
}

package loamstream.drm.uger

import loamstream.drm.AccountingClient
import loamstream.drm.Queue
import loamstream.util.ValueBox
import loamstream.conf.UgerConfig
import scala.util.Try
import scala.concurrent.duration.Duration
import loamstream.util.RunResults
import scala.util.Success
import loamstream.model.execute.Resources.DrmResources
import loamstream.util.RetryingCommandInvoker
import loamstream.model.jobs.TerminationReason
import scala.concurrent.Future
import loamstream.drm.DrmTaskId

/**
 * @author clint
 * Mar 15, 2017
 */
final class MockQacctAccountingClient(
    delegateFn: DrmTaskId => Try[RunResults],
    ugerConfig: UgerConfig = UgerConfig(),
    delayStart: Duration = RetryingCommandInvoker.defaultDelayStart,
    delayCap: Duration = RetryingCommandInvoker.defaultDelayCap) extends AccountingClient {
  
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
      new RetryingCommandInvoker[DrmTaskId](ugerConfig.maxQacctRetries, "MOCK", wrappedDelegateFn, delayStart, delayCap)
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

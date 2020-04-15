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
import rx.lang.scala.schedulers.ComputationScheduler
import loamstream.drm.AccountingInfo
import scala.concurrent.ExecutionContext
import loamstream.drm.DrmTaskArray

/**
 * @author clint
 * Mar 15, 2017
 */
final class MockQacctAccountingClient(
    delegateFn: Either[DrmTaskId, DrmTaskArray] => Try[RunResults],
    ugerConfig: UgerConfig = UgerConfig(),
    delayStart: Duration = RetryingCommandInvoker.defaultDelayStart,
    delayCap: Duration = RetryingCommandInvoker.defaultDelayCap) extends AccountingClient {
  
  private val timesGetQacctOutputForInvokedBox: ValueBox[Int] = ValueBox(0)

  private val timesGetResourceUsageInvokedBox: ValueBox[Int] = ValueBox(0)
  
  private val timesGetTerminationReasonInvokedBox: ValueBox[Int] = ValueBox(0)
  
  private val timesGetAccountingInfoFromTaskArrayInvokedBox: ValueBox[Int] = ValueBox(0)
  
  def timesGetQacctOutputForInvoked: Int = timesGetQacctOutputForInvokedBox()
    
  def timesGetResourceUsageInvoked: Int = timesGetResourceUsageInvokedBox()
  
  def timesGetTerminationReasonInvoked: Int = timesGetTerminationReasonInvokedBox()

  def timesGetAccountingInfoFromTaskArrayInvoked: Int = timesGetAccountingInfoFromTaskArrayInvokedBox()
  
  private val actualDelegate = {
    val fakeBinaryName = "MOCK"
    
    val wrappedDelegateFn: Either[DrmTaskId, DrmTaskArray] => Try[RunResults] = { taskId =>
      timesGetQacctOutputForInvokedBox.mutate(_ + 1)

      delegateFn(taskId)
    }

    import scala.concurrent.ExecutionContext.Implicits.global
    
    val invoker = {
      new RetryingCommandInvoker[Either[DrmTaskId, DrmTaskArray]](
          ugerConfig.maxQacctRetries, 
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
  
  override def getAccountingInfo(taskArray: DrmTaskArray): Future[Map[DrmTaskId, AccountingInfo]] = {
    timesGetAccountingInfoFromTaskArrayInvokedBox.mutate(_ + 1)
    
    actualDelegate.getAccountingInfo(taskArray)
  }
}

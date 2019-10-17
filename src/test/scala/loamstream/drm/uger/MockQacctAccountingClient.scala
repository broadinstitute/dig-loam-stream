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

/**
 * @author clint
 * Mar 15, 2017
 */
final class MockQacctAccountingClient(
    delegateFn: String => Try[RunResults],
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
    
    val wrappedDelegateFn: String => Try[RunResults] = { jobId =>
      timesGetQacctOutputForInvokedBox.mutate(_ + 1)

      delegateFn(jobId)
    }

    import scala.concurrent.ExecutionContext.Implicits.global
    
    val invoker = {
      new RetryingCommandInvoker[String](ugerConfig.maxQacctRetries, "MOCK", wrappedDelegateFn, delayStart, delayCap)
    }
    
    new QacctAccountingClient(invoker)
  }

  override def getResourceUsage(jobId: String): Future[DrmResources] = {
    timesGetResourceUsageInvokedBox.mutate(_ + 1)
    
    actualDelegate.getResourceUsage(jobId)
  }
  
  override def getTerminationReason(jobId: String): Future[Option[TerminationReason]] = {
    timesGetTerminationReasonInvokedBox.mutate(_ + 1)
    
    actualDelegate.getTerminationReason(jobId)
  }
}

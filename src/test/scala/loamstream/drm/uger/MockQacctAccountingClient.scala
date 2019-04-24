package loamstream.drm.uger

import loamstream.drm.AccountingClient
import loamstream.drm.Queue
import loamstream.util.ValueBox
import loamstream.conf.UgerConfig
import scala.util.Try
import scala.concurrent.duration.Duration
import loamstream.util.RunResults
import scala.util.Success

/**
 * @author clint
 * Mar 15, 2017
 */
final class MockQacctAccountingClient(
    delegateFn: String => Try[RunResults],
    ugerConfig: UgerConfig = UgerConfig(),
    delayStart: Duration = AccountingClient.defaultDelayStart,
    delayCap: Duration = AccountingClient.defaultDelayCap) extends AccountingClient {
  
  private val timesGetQacctOutputForInvokedBox: ValueBox[Int] = ValueBox(0)

  private val timesGetExecutionNodeInvokedBox: ValueBox[Int] = ValueBox(0)

  private val timesGetQueueInvokedBox: ValueBox[Int] = ValueBox(0)
  
  def timesGetQacctOutputForInvoked: Int = timesGetQacctOutputForInvokedBox()

  def timesGetExecutionNodeInvoked: Int = timesGetExecutionNodeInvokedBox()

  def timesGetQueueInvoked: Int = timesGetQueueInvokedBox()

  private val actualDelegate = {
    val fakeBinaryName = "MOCK"
    
    val wrappedDelegateFn: String => Try[RunResults] = { jobId =>
      timesGetQacctOutputForInvokedBox.mutate(_ + 1)

      delegateFn(jobId)
    }

    new QacctAccountingClient(ugerConfig, fakeBinaryName, wrappedDelegateFn, delayStart, delayCap)
  }

  override def getExecutionNode(jobId: String): Option[String] = {
    timesGetExecutionNodeInvokedBox.mutate(_ + 1)

    actualDelegate.getExecutionNode(jobId)
  }

  override def getQueue(jobId: String): Option[Queue] = {
    timesGetQueueInvokedBox.mutate(_ + 1)

    actualDelegate.getQueue(jobId)
  }
}

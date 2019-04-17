package loamstream.drm.uger

import loamstream.drm.AccountingClient
import loamstream.drm.Queue
import loamstream.util.ValueBox
import loamstream.conf.UgerConfig
import scala.util.Try
import scala.concurrent.duration.Duration

/**
 * @author clint
 * Mar 15, 2017
 */
final class MockQacctAccountingClient(
    delegateFn: String => Seq[String],
    ugerConfig: UgerConfig = UgerConfig(),
    delayStart: Duration = QacctAccountingClient.defaultDelayStart,
    delayCap: Duration = QacctAccountingClient.defaultDelayCap) extends AccountingClient {
  
  private val timesGetQacctOutputForInvokedBox: ValueBox[Int] = ValueBox(0)

  private val timesGetExecutionNodeInvokedBox: ValueBox[Int] = ValueBox(0)

  private val timesGetQueueInvokedBox: ValueBox[Int] = ValueBox(0)
  
  def timesGetQacctOutputForInvoked: Int = timesGetQacctOutputForInvokedBox()

  def timesGetExecutionNodeInvoked: Int = timesGetExecutionNodeInvokedBox()

  def timesGetQueueInvoked: Int = timesGetQueueInvokedBox()

  private val actualDelegate = {
    val wrappedDelegateFn: String => Seq[String] = { jobId =>
      timesGetQacctOutputForInvokedBox.mutate(_ + 1)

      delegateFn(jobId)
    }

    new QacctAccountingClient(ugerConfig, wrappedDelegateFn, delayStart, delayCap)
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

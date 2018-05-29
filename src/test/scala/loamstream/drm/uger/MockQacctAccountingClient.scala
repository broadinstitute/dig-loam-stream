package loamstream.drm.uger

import loamstream.drm.AccountingClient
import loamstream.drm.Queue
import loamstream.util.ValueBox

/**
 * @author clint
 * Mar 15, 2017
 */
final class MockQacctAccountingClient(delegateFn: String => Seq[String]) extends AccountingClient {
  val timesGetQacctOutputForInvoked: ValueBox[Int] = ValueBox(0)

  val timesGetExecutionNodeInvoked: ValueBox[Int] = ValueBox(0)

  val timesGetQueueInvoked: ValueBox[Int] = ValueBox(0)

  private val actualDelegate = {
    val wrappedDelegateFn: String => Seq[String] = { jobId =>
      timesGetQacctOutputForInvoked.mutate(_ + 1)

      delegateFn(jobId)
    }

    new QacctAccountingClient(wrappedDelegateFn)
  }

  override def getExecutionNode(jobId: String): Option[String] = {
    timesGetExecutionNodeInvoked.mutate(_ + 1)

    actualDelegate.getExecutionNode(jobId)
  }

  override def getQueue(jobId: String): Option[Queue] = {
    timesGetQueueInvoked.mutate(_ + 1)

    actualDelegate.getQueue(jobId)
  }
}

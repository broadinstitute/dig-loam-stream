package loamstream.uger

import loamstream.util.ValueBox
import loamstream.oracle.uger.Queue

/**
 * @author clint
 * Mar 15, 2017
 */
final class MockUgerClient(delegateFn: String => Seq[String]) extends UgerClient {
  val timesGetQacctOutputForInvoked: ValueBox[Int] = ValueBox(0)

  val timesGetExecutionNodeInvoked: ValueBox[Int] = ValueBox(0)

  val timesGetQueueInvoked: ValueBox[Int] = ValueBox(0)

  private val actualDelegate = {
    val wrappedDelegateFn: String => Seq[String] = { jobId =>
      timesGetQacctOutputForInvoked.mutate(_ + 1)

      delegateFn(jobId)
    }

    new UgerClient.QacctUgerClient(wrappedDelegateFn)
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

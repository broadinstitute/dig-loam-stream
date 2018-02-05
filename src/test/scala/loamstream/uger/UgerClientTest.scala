package loamstream.uger

import java.time.Instant

import scala.util.Try

import org.scalatest.FunSuite

import loamstream.model.execute.Resources.UgerResources
import loamstream.model.quantities.CpuTime
import loamstream.model.quantities.Memory
import loamstream.util.Tries

/**
 * @author clint
 * Mar 20, 2017
 */
final class UgerClientTest extends FunSuite {

  private val resources = UgerResources(
      Memory.inGb(123.45),
      CpuTime.inSeconds(456.789),
      node = None,
      queue = None,
      Instant.now,
      Instant.now)

  test("fillInAccountingFieldsIfNecessary") {
    import UgerClient.fillInAccountingFieldsIfNecessary
    import QacctTestHelpers.actualQacctOutput

    val expectedQueue = Queue.Broad

    val expectedNode = "foo.example.com"

    val qacctOutput = actualQacctOutput(Option(expectedQueue), Option(expectedNode))

    val mockClient = new MockAccountingClient(_ => qacctOutput)

    val failure = Tries.failure("blarg")

    assert(fillInAccountingFieldsIfNecessary(mockClient, "12334")(failure) === failure)

    def doTest(ugerStatus: UgerStatus): Unit = {
      val mockClient = new MockAccountingClient(_ => actualQacctOutput(Some(Queue.Broad), Some("foo.example.com")))

      val result = fillInAccountingFieldsIfNecessary(mockClient, "12334")(Try(ugerStatus))

      if(ugerStatus.notFinished) {
        assert(result.get === ugerStatus)
      } else {
        if(ugerStatus.resourcesOpt.isDefined) {
          val resultResources = result.get.resourcesOpt.get

          assert(resultResources.queue === Some(expectedQueue))
          assert(resultResources.node === Some(expectedNode))
        } else {
          assert(result.get.resourcesOpt === None)
        }
      }
    }

    doTest(UgerStatus.Done)
    doTest(UgerStatus.DoneUndetermined(Some(resources)))
    doTest(UgerStatus.CommandResult(0, None))
    doTest(UgerStatus.CommandResult(1, None))
    doTest(UgerStatus.CommandResult(0, Some(resources)))
    doTest(UgerStatus.CommandResult(1, Some(resources)))
    doTest(UgerStatus.Failed(Some(resources)))
    doTest(UgerStatus.Queued)
    doTest(UgerStatus.QueuedHeld)
    doTest(UgerStatus.Requeued)
    doTest(UgerStatus.RequeuedHeld)
    doTest(UgerStatus.Running)
    doTest(UgerStatus.Suspended(Some(resources)))
    doTest(UgerStatus.Undetermined(Some(resources)))
  }
}

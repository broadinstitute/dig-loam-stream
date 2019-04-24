package loamstream.drm

import java.time.Instant

import scala.util.Try

import org.scalatest.FunSuite

import loamstream.drm.uger.MockQacctAccountingClient
import loamstream.drm.uger.QacctTestHelpers
import loamstream.model.execute.Resources.UgerResources
import loamstream.model.quantities.CpuTime
import loamstream.model.quantities.Memory
import loamstream.util.Tries
import loamstream.conf.UgerConfig



/**
 * @author clint
 * Mar 20, 2017
 */
final class DrmClientTest extends FunSuite {

  private val resources = UgerResources(
      Memory.inGb(123.45),
      CpuTime.inSeconds(456.789),
      node = None,
      queue = None,
      Instant.now,
      Instant.now)

  test("fillInAccountingFieldsIfNecessary - invoking AccountingClient fails") {
    import DrmClient.fillInAccountingFieldsIfNecessary
    import QacctTestHelpers.actualQacctOutput
    import QacctTestHelpers.successfulRun

    def doTestWithRetries(maxRetries: Int): Unit = {
      val ugerConfig = UgerConfig().copy(maxQacctRetries = maxRetries)
      
      val mockClientThatFails = {
        import scala.concurrent.duration._
        
        new MockQacctAccountingClient(_ => Tries.failure("quxx!"), ugerConfig, delayStart = 0.001.seconds)
      }
  
      val failure = Tries.failure("blarg")
  
      assert(fillInAccountingFieldsIfNecessary(mockClientThatFails, "12334")(failure) === failure)
  
      def doTest(ugerStatus: DrmStatus): Unit = {
        //The input should be unchanged if invoking the accounting client failed
        val result = fillInAccountingFieldsIfNecessary(mockClientThatFails, "12334")(Try(ugerStatus))
  
        assert(result.get === ugerStatus)
      }
  
      doTest(DrmStatus.Done)
      doTest(DrmStatus.DoneUndetermined(Some(resources)))
      doTest(DrmStatus.CommandResult(0, None))
      doTest(DrmStatus.CommandResult(1, None))
      doTest(DrmStatus.CommandResult(0, Some(resources)))
      doTest(DrmStatus.CommandResult(1, Some(resources)))
      doTest(DrmStatus.Failed(Some(resources)))
      doTest(DrmStatus.Queued)
      doTest(DrmStatus.QueuedHeld)
      doTest(DrmStatus.Requeued)
      doTest(DrmStatus.RequeuedHeld)
      doTest(DrmStatus.Running)
      doTest(DrmStatus.Suspended(Some(resources)))
      doTest(DrmStatus.Undetermined(Some(resources)))
    }
    
    doTestWithRetries(0)
    doTestWithRetries(1)
    doTestWithRetries(5)
  }
      
  test("fillInAccountingFieldsIfNecessary") {
    import DrmClient.fillInAccountingFieldsIfNecessary
    import QacctTestHelpers.actualQacctOutput
    import QacctTestHelpers.successfulRun

    val expectedQueue = Queue("broad")

    val expectedNode = "foo.example.com"

    val qacctOutput = actualQacctOutput(Option(expectedQueue), Option(expectedNode))

    val mockClient = new MockQacctAccountingClient(_ => successfulRun(stdout = qacctOutput))

    val failure = Tries.failure("blarg")

    assert(fillInAccountingFieldsIfNecessary(mockClient, "12334")(failure) === failure)

    def doTest(ugerStatus: DrmStatus): Unit = {
      val mockClient = new MockQacctAccountingClient(
          _ => successfulRun(stdout = actualQacctOutput(Some(Queue("broad")), Some("foo.example.com"))))

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

    doTest(DrmStatus.Done)
    doTest(DrmStatus.DoneUndetermined(Some(resources)))
    doTest(DrmStatus.CommandResult(0, None))
    doTest(DrmStatus.CommandResult(1, None))
    doTest(DrmStatus.CommandResult(0, Some(resources)))
    doTest(DrmStatus.CommandResult(1, Some(resources)))
    doTest(DrmStatus.Failed(Some(resources)))
    doTest(DrmStatus.Queued)
    doTest(DrmStatus.QueuedHeld)
    doTest(DrmStatus.Requeued)
    doTest(DrmStatus.RequeuedHeld)
    doTest(DrmStatus.Running)
    doTest(DrmStatus.Suspended(Some(resources)))
    doTest(DrmStatus.Undetermined(Some(resources)))
  }
}

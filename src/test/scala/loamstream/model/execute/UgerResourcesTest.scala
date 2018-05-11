package loamstream.model.execute

import org.scalatest.FunSuite
import java.time.Instant
import loamstream.drm.Queue
import loamstream.drm.uger.UgerException
import loamstream.drm.uger.UgerException
import loamstream.model.quantities.Memory
import loamstream.model.quantities.CpuTime

/**
 * @author clint
 * Mar 13, 2017
 */
final class UgerResourcesTest extends FunSuite {

  import Resources.DrmResources
  import scala.concurrent.duration._

  private val now = Instant.now

  test("elapsedTime") {
    //Elapsed time is zero
    val tookNoTime = DrmResources(
      Memory.inGb(1),
      CpuTime(42.seconds),
      node = None,
      queue = None,
      startTime = now,
      endTime = now)

    assert(tookNoTime.elapsedTime === 0.seconds)

    def doTestWithOffset(offsetInMillis: Long): Unit = {
      val resources = DrmResources(
        Memory.inGb(1),
        CpuTime(42.seconds),
        node = None,
        queue = None,
        startTime = now,
        endTime = now.plusMillis(offsetInMillis))

      assert(resources.elapsedTime === offsetInMillis.milliseconds)
    }

    doTestWithOffset(0)
    doTestWithOffset(1)
    doTestWithOffset(1000)
    doTestWithOffset(42)
  }

  test("withNode/withQueue") {
    val r = DrmResources(
      Memory.inGb(1),
      CpuTime(42.seconds),
      node = None,
      queue = None,
      startTime = now,
      endTime = now)

    val withNode = r.withNode("foo.example.com")

    assert(withNode.memory === r.memory)
    assert(withNode.cpuTime === r.cpuTime)
    assert(withNode.node === Some("foo.example.com"))
    assert(withNode.queue === None)
    assert(withNode.startTime === r.startTime)
    assert(withNode.endTime === r.endTime)

    val q: Queue = Queue("broad")
    
    val withNodeAndQueue = withNode.withQueue(q)

    assert(withNodeAndQueue.memory === r.memory)
    assert(withNodeAndQueue.cpuTime === r.cpuTime)
    assert(withNodeAndQueue.node === Some("foo.example.com"))
    assert(withNodeAndQueue.queue === Some(q))
    assert(withNodeAndQueue.startTime === r.startTime)
    assert(withNodeAndQueue.endTime === r.endTime)
  }
}

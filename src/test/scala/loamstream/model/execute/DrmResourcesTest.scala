package loamstream.model.execute

import java.time.Instant

import org.scalatest.FunSuite

import loamstream.drm.Queue
import loamstream.model.execute.Resources.DrmResources
import loamstream.model.execute.Resources.LsfResources
import loamstream.model.quantities.CpuTime
import loamstream.model.quantities.Memory
import java.time.LocalDateTime
import loamstream.model.execute.Resources.SlurmResources

/**
 * @author clint
 * Mar 13, 2017
 */
final class DrmResourcesTest extends FunSuite {

  import Resources.UgerResources
  import scala.concurrent.duration._

  private val now = LocalDateTime.now

  private val rawResourceData = "lalala lalala blah lalala"
  
  import DrmResources.ResourcesMaker

  test("elapsedTime") {
    def doTest[R <: DrmResources](makeResources: ResourcesMaker[R]): Unit = {
      //Elapsed time is zero
      val tookNoTime = makeResources(
        Memory.inGb(1),
        CpuTime(42.seconds),
        None,
        None,
        now,
        now,
        Some(rawResourceData))
  
      assert(tookNoTime.elapsedTime === 0.seconds)
  
      def doTestWithOffset(offsetInMillis: Long): Unit = {
        val resources = makeResources(
          Memory.inGb(1),
          CpuTime(42.seconds),
          None,
          None,
          now,
          now.plusNanos(offsetInMillis * 1000 * 1000),
          Some(rawResourceData))
  
        assert(resources.elapsedTime === offsetInMillis.milliseconds)
      }
  
      doTestWithOffset(0)
      doTestWithOffset(1)
      doTestWithOffset(1000)
      doTestWithOffset(42)
    }
    
    doTest(UgerResources.apply)
    doTest(LsfResources.apply)
    doTest(SlurmResources.apply)
  }

  test("withNode/withQueue") {
    def doTest[R <: DrmResources](makeResources: ResourcesMaker[R]): Unit = {
      val r = makeResources(
        Memory.inGb(1),
        CpuTime(42.seconds),
        None,
        None,
        now,
        now,
        Some(rawResourceData))
  
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
    
    doTest(UgerResources.apply)
    doTest(LsfResources.apply)
    doTest(SlurmResources.apply)
  }
}

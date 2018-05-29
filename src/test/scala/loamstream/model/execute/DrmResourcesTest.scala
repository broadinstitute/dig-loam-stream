package loamstream.model.execute

import java.time.Instant

import org.scalatest.FunSuite

import loamstream.drm.Queue
import loamstream.model.execute.Resources.DrmResources
import loamstream.model.execute.Resources.LsfResources
import loamstream.model.quantities.CpuTime
import loamstream.model.quantities.Memory

/**
 * @author clint
 * Mar 13, 2017
 */
final class DrmResourcesTest extends FunSuite {

  import Resources.UgerResources
  import scala.concurrent.duration._

  private val now = Instant.now

  private type ResourceMaker[R] = (Memory, CpuTime, Option[String], Option[Queue], Instant, Instant) => R
  
  test("elapsedTime") {
    def doTest[R <: DrmResources](makeResources: ResourceMaker[R]): Unit = {
      //Elapsed time is zero
      val tookNoTime = makeResources(
        Memory.inGb(1),
        CpuTime(42.seconds),
        None,
        None,
        now,
        now)
  
      assert(tookNoTime.elapsedTime === 0.seconds)
  
      def doTestWithOffset(offsetInMillis: Long): Unit = {
        val resources = makeResources(
          Memory.inGb(1),
          CpuTime(42.seconds),
          None,
          None,
          now,
          now.plusMillis(offsetInMillis))
  
        assert(resources.elapsedTime === offsetInMillis.milliseconds)
      }
  
      doTestWithOffset(0)
      doTestWithOffset(1)
      doTestWithOffset(1000)
      doTestWithOffset(42)
    }
    
    doTest(UgerResources.apply)
    doTest(LsfResources.apply)
  }

  test("withNode/withQueue") {
    def doTest[R <: DrmResources](makeResources: ResourceMaker[R]): Unit = {
      val r = UgerResources(
        memory = Memory.inGb(1),
        cpuTime = CpuTime(42.seconds),
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
    
    doTest(UgerResources.apply)
    doTest(LsfResources.apply)
  }
}

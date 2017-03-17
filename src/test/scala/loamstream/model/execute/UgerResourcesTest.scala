package loamstream.model.execute

import org.scalatest.FunSuite
import java.time.Instant
import loamstream.uger.Queue
import loamstream.uger.UgerException
import loamstream.uger.UgerException

/**
 * @author clint
 * Mar 13, 2017
 */
final class UgerResourcesTest extends FunSuite {
  //scalastyle:off magic.number
  
  import Resources.UgerResources
  import scala.concurrent.duration._

  private val now = Instant.now

  test("elapsedTime") {
    //Elapsed time is zero
    val tookNoTime = UgerResources(
      Memory.inGb(1),
      CpuTime(42.seconds),
      node = None,
      queue = None,
      startTime = now,
      endTime = now)

    assert(tookNoTime.elapsedTime === 0.seconds)

    def doTestWithOffset(offsetInMillis: Long): Unit = {
      val resources = UgerResources(
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
    val r = UgerResources(
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

    def doTestWithQueue(q: Queue): Unit = {
      val withNodeAndQueue = withNode.withQueue(q)

      assert(withNodeAndQueue.memory === r.memory)
      assert(withNodeAndQueue.cpuTime === r.cpuTime)
      assert(withNodeAndQueue.node === Some("foo.example.com"))
      assert(withNodeAndQueue.queue === Some(q))
      assert(withNodeAndQueue.startTime === r.startTime)
      assert(withNodeAndQueue.endTime === r.endTime)
    }

    doTestWithQueue(Queue.Short)
    doTestWithQueue(Queue.Long)
  }

  test("fromMap - real-world data") {
    import UgerResources.fromMap

    assert(fromMap(null).isFailure) //scalastyle:ignore null
      
    intercept[UgerException] {
      fromMap(null).get //scalastyle:ignore null
    }
      
    assert(fromMap(Map.empty).isFailure)
    
    intercept[UgerException] {
      fromMap(Map.empty).get
    }

    val r = fromMap(realWorldMap).get
    
    assert(r.cpuTime === CpuTime(1.9867.seconds))
    assert(r.memory === Memory.inGb(0.0141))
    assert(r.node === None)
    assert(r.queue === None)
    assert(r.startTime === Instant.ofEpochMilli(1488840619845L))
    assert(r.endTime === Instant.ofEpochMilli(1488840622397L))
  }
  
  test("fromMap - some missing fields") {
    import UgerResources.fromMap
    import UgerResources.Keys

    assert(fromMap(realWorldMap - Keys.cpu).isFailure)
    assert(fromMap(realWorldMap - Keys.mem).isFailure)
    assert(fromMap(realWorldMap - Keys.startTime).isFailure)
    assert(fromMap(realWorldMap - Keys.endTime).isFailure)
    
    intercept[UgerException] {
      fromMap(realWorldMap - Keys.cpu).get
    }
    
    intercept[UgerException] {
      fromMap(realWorldMap - Keys.mem).get
    }
    
    intercept[UgerException] {
      fromMap(realWorldMap - Keys.startTime).get
    }
    
    intercept[UgerException] {
      fromMap(realWorldMap - Keys.endTime).get
    }
  }
  
  test("fromMap - some malformed fields") {
    import UgerResources.fromMap
    import UgerResources.Keys

    def doTestWithBorkedKey(key: String): Unit = {
      assert(fromMap(realWorldMap + (key -> "asdf")).isFailure)
      
      intercept[UgerException] {
        fromMap(realWorldMap + (key -> "asdf")).get
      }
    }
    
    doTestWithBorkedKey(Keys.cpu)
    doTestWithBorkedKey(Keys.mem)
    doTestWithBorkedKey(Keys.startTime)
    doTestWithBorkedKey(Keys.endTime)
  }

  private val realWorldMap: Map[Any, Any] = Map(
    "acct_cpu" -> "1.9867",
    "acct_io" -> "0.0047",
    "acct_ioops" -> "3436.0000",
    "acct_iow" -> "0.0600",
    "acct_maxvmem" -> "177045504.0000",
    "acct_mem" -> "0.0141",
    "cpu" -> "1.9867",
    "end_time" -> "1488840622397.0000",
    "exit_status" -> "0.0000",
    "io" -> "0.0047",
    "ioops" -> "3436.0000",
    "iow" -> "0.0600",
    "maxvmem" -> "177045504.0000",
    "mem" -> "0.0141",
    "priority" -> "0.0000",
    "ru_idrss" -> "0.0000",
    "ru_inblock" -> "1144.0000",
    "ru_ismrss" -> "0.0000",
    "ru_isrss" -> "0.0000",
    "ru_ixrss" -> "0.0000",
    "ru_majflt" -> "2.0000",
    "ru_maxrss" -> "54328.0000",
    "ru_minflt" -> "45953.0000",
    "ru_msgrcv" -> "0.0000",
    "ru_msgsnd" -> "0.0000",
    "ru_nivcsw" -> "161.0000",
    "ru_nsignals" -> "0.0000",
    "ru_nswap" -> "0.0000",
    "ru_nvcsw" -> "748.0000",
    "ru_oublock" -> "1760.0000",
    "ru_stime" -> "0.0900",
    "ru_utime" -> "1.8967",
    "ru_wallclock" -> "2.5520",
    "signal" -> "0.0000",
    "start_time" -> "1488840619845.0000",
    "submission_time" -> "1488840615805.0000",
    "vmem" -> "0.0000",
    "wallclock" -> "2.7110")
    
  //scalastyle:on magic.number
}

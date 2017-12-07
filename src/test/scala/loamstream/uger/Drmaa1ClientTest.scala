package loamstream.uger

import java.time.Instant

import org.ggf.drmaa.JobInfo
import org.ggf.drmaa.JobTemplate
import org.scalatest.FunSuite

import loamstream.TestHelpers
import loamstream.conf.UgerConfig
import loamstream.model.execute.UgerSettings
import loamstream.model.quantities.CpuTime
import loamstream.model.quantities.Cpus
import loamstream.model.quantities.Memory


/**
 * @author clint
 * Mar 15, 2017
 */
final class Drmaa1ClientTest extends FunSuite {
  private val actualQacctOutput = QacctTestHelpers.actualQacctOutput(Some(Queue.Broad), Some("foo.example.com"))
  
  import Drmaa1ClientTest.LiteralJobInfo
  import loamstream.TestHelpers.path
  
  test("toResources - valid resource-usage data in JobInfo") {
    val mockUgerClient = new MockAccountingClient(_ => actualQacctOutput)
    
    val jobId = "12345"
    
    val jobInfo = LiteralJobInfo(jobId, realWorldResourceUsageMap)
    
    val r = Drmaa1Client.toResources(jobInfo).get
    
    import scala.concurrent.duration._
    
    assert(r.cpuTime === CpuTime(1.9867.seconds))
    assert(r.memory === Memory.inKb(54328))
    assert(r.node === None)
    assert(r.queue === None)
    assert(r.startTime === Instant.ofEpochMilli(1488840619845L))
    assert(r.endTime === Instant.ofEpochMilli(1488840622397L))
  }
  
  test("toResources - incomplete resource-usage data in JobInfo") {
    val mockUgerClient = new MockAccountingClient(_ => actualQacctOutput)
    
    val jobId = "12345"
    
    val jobInfo = LiteralJobInfo(jobId, realWorldResourceUsageMap - "cpu")
    
    //Should fail; incomplete info from JobInfo
    assert(Drmaa1Client.toResources(jobInfo).isFailure)
    
    assert(mockUgerClient.timesGetExecutionNodeInvoked() == 0)
    assert(mockUgerClient.timesGetQueueInvoked() == 0)
  }
  
  private val realWorldResourceUsageMap: Map[Any,Any] = Map(
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

  test("nativeSpec") {
    import Drmaa1Client.nativeSpec
    import TestHelpers.path
    
    val bogusPath = path("/foo/bar/baz")
    
    val ugerConfig = UgerConfig(workDir = bogusPath, maxNumJobs = 41)
        
    val ugerSettings = UgerSettings(
        cores = Cpus(42),
        memoryPerCore = Memory.inGb(17),
        maxRunTime = CpuTime.inHours(33))
        
    assert(ugerSettings.cores !== UgerDefaults.cores)
    assert(ugerSettings.memoryPerCore !== UgerDefaults.memoryPerCore)
    assert(ugerSettings.maxRunTime !== UgerDefaults.maxRunTime)
    
    val actualNativeSpec = nativeSpec(ugerSettings)
    
    val expected = "-cwd -shell y -b n -binding linear:42 -pe smp 42 -q broad -l h_rt=33:0:0,h_vmem=17g"
    
    assert(actualNativeSpec === expected)
  }
}

object Drmaa1ClientTest {
  private final case class LiteralJobInfo(
      getJobId: String,
      resourceUsage: Map[Any, Any],
      hasExited: Boolean = true,
      getExitStatus: Int = 0,
      hasSignaled: Boolean = false,
      getTerminatingSignal: String = "",
      hasCoreDump: Boolean = false,
      wasAborted: Boolean = false) extends JobInfo {
    
    override def getResourceUsage: java.util.Map[_, _] = {
      import scala.collection.JavaConverters._
      
      resourceUsage.asJava
    }
  }
}

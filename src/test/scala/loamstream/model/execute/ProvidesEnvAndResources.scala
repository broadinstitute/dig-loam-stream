package loamstream.model.execute

import java.time.Instant

import org.scalatest.FunSuite

import loamstream.TestHelpers
import loamstream.model.execute.Environment.Uger
import loamstream.model.execute.Resources.GoogleResources
import loamstream.model.execute.Resources.LocalResources
import loamstream.model.execute.Resources.DrmResources
import loamstream.model.jobs.Execution
import loamstream.model.jobs.JobResult
import loamstream.model.jobs.JobStatus
import loamstream.model.quantities.CpuTime
import loamstream.model.quantities.Cpus
import loamstream.model.quantities.Memory
import loamstream.drm.Queue
import loamstream.model.jobs.RunData
import loamstream.model.jobs.LJob
import loamstream.uger.UgerDefaults

/**
 * @author kyuksel
 *         date: 3/11/17
 */
trait ProvidesEnvAndResources extends FunSuite {
  
  private val broadQueue = Queue("broad")
  
  val mockCmd: String = "R --vanilla --args ancestry_pca_scores.tsv < plot_ancestry_pca.r"
  val mockSettings: DrmSettings = DrmSettings(
      Cpus(4), Memory.inGb(8), UgerDefaults.maxRunTime, queue = Option(broadQueue))
      
  val mockGoogleSettings: GoogleSettings = GoogleSettings("asdf")
  val mockEnv: Environment = Uger(mockSettings)
  val mockStatus: JobStatus = JobStatus.Unknown
  val mockExitCode: Int = 999
  val mockResult: JobResult = JobResult.CommandResult(mockExitCode)

  val mockLocalResources: LocalResources = TestHelpers.localResources
  
  val mockUgerResources: DrmResources = {
    val mem = Memory.inGb(2.1)
    val cpu = CpuTime.inSeconds(12.34)
    val startTime = Instant.ofEpochMilli(64532) // scalastyle:ignore magic.number
    val endTime = Instant.ofEpochMilli(9345345) // scalastyle:ignore magic.number

    DrmResources(mem, cpu, Some("nodeName"), Some(broadQueue), startTime, endTime)
  }
  
  val mockGoogleResources: GoogleResources = GoogleResources("some-cluster-id", Instant.now, Instant.now)
  
  val mockResources: Resources = mockUgerResources

  val mockExecution: Execution = Execution(
      env = mockEnv, 
      status = mockStatus, 
      outputStreams = Some(TestHelpers.dummyOutputStreams))
      
  def mockRunData(job: LJob): RunData = {
    TestHelpers.runDataFrom(
        job = job, 
        status = mockStatus, 
        result = None, 
        resources = None, 
        outputStreams = Some(TestHelpers.dummyOutputStreams))
  }

  protected def assertEqualFieldsFor(actual: Iterable[Execution], expected: Iterable[Execution]): Unit = {
    assert(actual.map(_.env) === expected.map(_.env))
    assert(actual.map(_.cmd) === expected.map(_.cmd))
    assert(actual.map(_.result) === expected.map(_.result))
    assert(actual.map(_.settings) === expected.map(_.settings))
    assert(actual.map(_.resources) === expected.map(_.resources))
    assert(actual.map(_.outputStreams) === expected.map(_.outputStreams))
    assert(actual.map(_.outputs) === expected.map(_.outputs))
  }
}

//NB: Make fields from the trait available "statically" 
object ProvidesEnvAndResources extends ProvidesEnvAndResources

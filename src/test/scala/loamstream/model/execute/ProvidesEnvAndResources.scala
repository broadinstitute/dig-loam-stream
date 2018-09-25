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
import loamstream.drm.uger.UgerDefaults
import loamstream.model.execute.Resources.UgerResources
import loamstream.model.execute.Resources.LsfResources
import loamstream.drm.lsf.LsfDefaults

/**
 * @author kyuksel
 *         date: 3/11/17
 */
trait ProvidesEnvAndResources extends FunSuite {
  
  import TestHelpers.broadQueue
  
  val mockCmd: String = "R --vanilla --args ancestry_pca_scores.tsv < plot_ancestry_pca.r"
  val mockUgerSettings: DrmSettings = UgerDrmSettings(
      Cpus(4), Memory.inGb(8), UgerDefaults.maxRunTime, queue = Option(broadQueue), containerParams = None)
      
  val mockLsfSettings: DrmSettings = LsfDrmSettings(
      Cpus(4), Memory.inGb(8), LsfDefaults.maxRunTime, queue = None, containerParams = None)
      
  val mockGoogleSettings: GoogleSettings = GoogleSettings("asdf")
  val mockEnv: Environment = Uger(mockUgerSettings)
  val mockStatus: JobStatus = JobStatus.Unknown
  val mockExitCode: Int = 999
  val mockResult: JobResult = JobResult.CommandResult(mockExitCode)

  import TestHelpers.{ugerResources => mockUgerResources}
  import TestHelpers.{lsfResources => mockLsfResources}
  import TestHelpers.{localResources => mockLocalResources}
  import TestHelpers.{googleResources => mockGoogleResources}
  
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

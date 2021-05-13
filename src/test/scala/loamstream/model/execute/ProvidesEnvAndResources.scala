package loamstream.model.execute

import org.scalatest.FunSuite

import loamstream.TestHelpers
import loamstream.drm.lsf.LsfDefaults
import loamstream.drm.uger.UgerDefaults
import loamstream.googlecloud.ClusterConfig
import loamstream.model.jobs.Execution
import loamstream.model.jobs.JobResult
import loamstream.model.jobs.JobStatus
import loamstream.model.jobs.LJob
import loamstream.model.jobs.RunData
import loamstream.model.jobs.StoreRecord
import loamstream.model.quantities.Cpus
import loamstream.model.quantities.Memory

import scala.collection.compat._

/**
 * @author kyuksel
 *         date: 3/11/17
 */
trait ProvidesEnvAndResources extends FunSuite {
  
  import loamstream.TestHelpers.broadQueue
  
  val mockCmd: String = "R --vanilla --args ancestry_pca_scores.tsv < plot_ancestry_pca.r"
  val mockUgerSettings: DrmSettings = UgerDrmSettings(
      Cpus(4), Memory.inGb(8), UgerDefaults.maxRunTime, queue = Option(broadQueue), containerParams = None)
      
  val mockLsfSettings: DrmSettings = LsfDrmSettings(
      Cpus(4), Memory.inGb(8), LsfDefaults.maxRunTime, queue = None, containerParams = None)
      
  val mockGoogleSettings: GoogleSettings = GoogleSettings("asdf", ClusterConfig.default)
  val mockStatus: JobStatus = JobStatus.Unknown
  val mockExitCode: Int = 999
  val mockResult: JobResult = JobResult.CommandResult(mockExitCode)

  import loamstream.TestHelpers.{ ugerResources => mockUgerResources }
  
  val mockResources: Resources = mockUgerResources

  val mockExecution: Execution = Execution(
      settings = mockUgerSettings,
      status = mockStatus, 
      jobDir = Some(TestHelpers.dummyJobDir),
      terminationReason = None)
      
  def mockRunData(job: LJob): RunData = {
    TestHelpers.runDataFrom(
        job = job, 
        settings = LocalSettings,
        status = mockStatus, 
        result = None, 
        resources = None, 
        jobDir = Some(TestHelpers.dummyJobDir))
  }

  protected def assertEqualFieldsFor(actual: Iterable[Execution.Persisted], expected: Iterable[Execution]): Unit = {
    implicit final class StoreRecordIterableOps(srs: Iterable[StoreRecord]) {
      def sortedByLoc: Seq[StoreRecord] = srs.to(Seq).sortBy(_.loc)
    }
    
    assert(actual.map(_.envType) === expected.map(_.envType))
    assert(actual.map(_.cmd) === expected.map(_.cmd))
    assert(actual.map(_.status) === expected.map(_.status))
    assert(actual.map(_.result) === expected.map(_.result))
    // Sort to ignore order whole not forcing evaluation of any lazy fields, which could happen with .to(Set)
    assert(actual.map(_.outputs.sortedByLoc) === expected.map(_.outputs.sortedByLoc)) 
    assert(actual.map(_.jobDir) === expected.map(_.jobDir))
    assert(actual.map(_.terminationReason) === expected.map(_.terminationReason))
  }
}

//NB: Make fields from the trait available "statically" 
object ProvidesEnvAndResources extends ProvidesEnvAndResources

package loamstream.model.execute

import java.time.Instant

import loamstream.model.execute.ExecutionEnvironment.Uger
import loamstream.model.jobs.{Execution, JobResult, JobStatus}
import loamstream.uger.Queue
import org.scalatest.FunSuite
import loamstream.model.execute.Resources.UgerResources
import loamstream.TestHelpers
import loamstream.model.execute.Resources.LocalResources
import loamstream.model.execute.Resources.GoogleResources

/**
 * @author kyuksel
 *         date: 3/11/17
 */
trait ProvidesEnvAndResources extends FunSuite {

  val mockEnv: ExecutionEnvironment = Uger
  val mockCmd: String = "R --vanilla --args ancestry_pca_scores.tsv < plot_ancestry_pca.r"
  val mockSettings: Settings = {
    val mem = 8
    val cpu = 4

    UgerSettings(mem, cpu, Queue.Short)
  }
  val mockStatus: JobStatus = JobStatus.Unknown
  val mockExitCode: Int = 999
  val mockResult: JobResult = JobResult.CommandResult(mockExitCode)

  val mockLocalResources: LocalResources = TestHelpers.localResources
  
  val mockUgerResources: UgerResources = {
    val mem = Memory.inGb(2.1)
    val cpu = CpuTime.inSeconds(12.34)
    val startTime = Instant.ofEpochMilli(64532) // scalastyle:ignore magic.number
    val endTime = Instant.ofEpochMilli(9345345) // scalastyle:ignore magic.number

    UgerResources(mem, cpu, Some("nodeName"), Some(Queue.Long), startTime, endTime)
  }
  
  val mockGoogleResources: GoogleResources = GoogleResources("some-cluster-id", Instant.now, Instant.now)
  
  val mockResources: Resources = mockUgerResources

  protected def assertEqualFieldsFor(actual: Iterable[Execution], expected: Iterable[Execution]): Unit = {
    assert(actual.map(_.env) === expected.map(_.env))
    assert(actual.map(_.cmd) === expected.map(_.cmd))
    assert(actual.map(_.result) === expected.map(_.result))
    assert(actual.map(_.settings) === expected.map(_.settings))
    assert(actual.map(_.resources) === expected.map(_.resources))
    assert(actual.map(_.outputs) === expected.map(_.outputs))
  }
}

//NB: Make fields from the trait available "statically" 
object ProvidesEnvAndResources extends ProvidesEnvAndResources

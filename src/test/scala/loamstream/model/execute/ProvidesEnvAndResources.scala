package loamstream.model.execute

import java.time.Instant

import loamstream.model.execute.ExecutionEnvironment.Uger
import loamstream.model.jobs.Execution
import loamstream.uger.Queue
import org.scalatest.FunSuite
import loamstream.model.execute.Resources.UgerResources

/**
 * @author kyuksel
 *         date: 3/11/17
 */
trait ProvidesEnvAndResources extends FunSuite {
  protected val mockEnv: ExecutionEnvironment = Uger
  protected val mockSettings: Settings = {
    val mem = 8
    val cpu = 4

    UgerSettings(mem, cpu, Queue.Short)
  }
  protected val mockResources: Resources = {
    val mem = Memory.inGb(2.1)
    val cpu = CpuTime.inSeconds(12.34)
    // scalastyle:off magic.number
    val startTime = Instant.ofEpochMilli(64532)
    val endTime = Instant.ofEpochMilli(9345345)
    // scalastyle:on magic.number
    UgerResources(mem, cpu, Some("nodeName"), Some(Queue.Long), startTime, endTime)
  }

  protected def assertEqualFieldsFor(actual: Set[Execution], expected: Set[Execution]): Unit = {
    assert(actual.map(_.env) === expected.map(_.env))
    assert(actual.map(_.exitState) === expected.map(_.exitState))
    assert(actual.map(_.settings) === expected.map(_.settings))
    assert(actual.map(_.resources) === expected.map(_.resources))
    assert(actual.map(_.outputs) === expected.map(_.outputs))
  }

  protected def assertEqualFieldsFor(actual: Option[Execution], expected: Option[Execution]): Unit = {
    assert(actual.map(_.env) === expected.map(_.env))
    assert(actual.map(_.exitState) === expected.map(_.exitState))
    assert(actual.map(_.settings) === expected.map(_.settings))
    assert(actual.map(_.resources) === expected.map(_.resources))
    assert(actual.map(_.outputs) === expected.map(_.outputs))
  }
}

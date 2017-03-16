package loamstream.model.execute

import java.time.Instant

import loamstream.model.execute.ExecutionEnvironment.Uger
import loamstream.uger.Queue

/**
 * @author kyuksel
 *         date: 3/11/17
 */
trait ProvidesEnvAndResources {
  protected val mockEnv: ExecutionEnvironment = Uger
  protected val mockSettings: Settings = {
    val mem = 8
    val cpu = 4

    UgerSettings(mem, cpu, Queue.Short)
  }
  protected val mockResources: Resources = {
    val mem = Some(2.1F)
    val cpu = Some(12.34F)
    // scalastyle:off magic.number
    val startTime = Some(Instant.ofEpochMilli(64532))
    val endTime = Some(Instant.ofEpochMilli(9345345))
    // scalastyle:on magic.number
    UgerResources(mem, cpu, Some("nodeName"), Some(Queue.Long), startTime, endTime)
  }
}

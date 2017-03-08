package loamstream.db.slick

import java.sql.Timestamp

import loamstream.googlecloud.GoogleSettings
import loamstream.model.execute.ExecutionEnvironment.{Google, Local, Uger}
import loamstream.model.execute.{ExecutionEnvironment, LocalSettings, Settings}
import loamstream.uger.{Queue, UgerSettings}

/**
 * @author kyuksel
 *         date: 3/7/17
 */
final case class SettingRow(executionId: Int,
                            env: String,
                            mem: Option[Float],
                            cpu: Option[Float],
                            startTime: Option[Timestamp],
                            endTime: Option[Timestamp],
                            node: Option[String],
                            queue: Option[String]) {

  def this(executionId: Int, env: String, settings: Settings) = {
    this(
      executionId,
      env,
      settings.mem,
      settings.cpu,
      settings.startTime.map(Timestamp.from),
      settings.endTime.map(Timestamp.from),
      settings.node,
      settings.queue.map(_.toString))
  }

  def toSettings: Settings = {
    ExecutionEnvironment.fromString(env) match {
      case Local => LocalSettings(mem, cpu, startTime.map(_.toInstant), endTime.map(_.toInstant))
      case Uger => UgerSettings(mem, cpu, startTime.map(_.toInstant), endTime.map(_.toInstant),
        node, queue.flatMap(Queue.fromString))
      case Google => GoogleSettings(mem, cpu, startTime.map(_.toInstant), endTime.map(_.toInstant), node)
    }
  }
}
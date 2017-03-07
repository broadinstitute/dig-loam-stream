package loamstream.db.slick

import java.sql.Timestamp

import loamstream.model.execute.Settings

/**
 * @author kyuksel
 *         date: 3/7/17
 */
final case class SettingRow(executionId: Int,
                            mem: Option[Float],
                            cpu: Option[Float],
                            startTime: Option[Timestamp],
                            endTime: Option[Timestamp],
                            node: Option[String],
                            queue: Option[String]) {

  def this(executionId: Int, settings: Settings) = {
    this(
      executionId,
      settings.mem,
      settings.cpu,
      settings.startTime.map(Timestamp.from),
      settings.endTime.map(Timestamp.from),
      settings.node,
      settings.queue.map(_.toString))
  }
}

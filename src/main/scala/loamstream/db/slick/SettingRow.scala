package loamstream.db.slick

import loamstream.model.execute.{Settings, UgerSettings}
import loamstream.uger.Queue

/**
 * @author kyuksel
 *         date: 3/7/17
 */
sealed trait SettingRow {
  def toSettings: Settings
}

final case class UgerSettingRow(executionId: Int,
                      mem: Int,
                      cpu: Int,
                      queue: String) {

  def toSettings: Settings = UgerSettings(mem, cpu, Queue.fromString(queue))
}

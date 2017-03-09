package loamstream.db.slick

import loamstream.model.execute.{GoogleSettings, LocalSettings, Settings, UgerSettings}
import loamstream.uger.Queue

/**
 * @author kyuksel
 *         date: 3/7/17
 */
sealed trait SettingRow {
  def toSettings: Settings
}

final case class LocalSettingRow(executionId: Int) {
  def toSettings = new LocalSettings
}

final case class UgerSettingRow(executionId: Int,
                      mem: Int,
                      cpu: Int,
                      queue: String) {

  def toSettings: Settings = UgerSettings(mem, cpu, Queue.fromString(queue))
}

final case class GoogleSettingRow(executionId: Int,
                                  mem: Int,
                                  cpu: Int,
                                  cluster: String) {

  def toSettings: Settings = GoogleSettings(mem, cpu, cluster)
}

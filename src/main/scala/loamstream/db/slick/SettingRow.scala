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

object SettingRow {
  def fromSettings(settings: Settings, executionId: Int): SettingRow = settings match {
    case LocalSettings(mem) => LocalSettingRow(executionId, mem)
    case UgerSettings(mem, cpu, queue) => UgerSettingRow(executionId, mem, cpu, queue.name)
    case GoogleSettings(cluster) => GoogleSettingRow(executionId, cluster)
  }
}

final case class LocalSettingRow(executionId: Int,
                                 mem: Option[Int]) extends SettingRow {

  override def toSettings: Settings = LocalSettings(mem)
}

final case class UgerSettingRow(executionId: Int,
                      mem: Int,
                      cpu: Int,
                      queue: String) extends SettingRow {

  //NB: TODO: .get :(
  override def toSettings: Settings = UgerSettings(mem, cpu, Queue.fromString(queue).get)
}

final case class GoogleSettingRow(executionId: Int,
                                  cluster: String) extends SettingRow {

  override def toSettings: Settings = GoogleSettings(cluster)
}

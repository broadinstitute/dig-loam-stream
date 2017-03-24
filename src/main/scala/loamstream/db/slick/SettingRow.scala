package loamstream.db.slick

import loamstream.model.execute.{GoogleSettings, LocalSettings, Settings, UgerSettings}
import loamstream.uger.Queue
import slick.driver.JdbcProfile

/**
 * @author kyuksel
 *         date: 3/7/17
 */
sealed trait SettingRow {
  def toSettings: Settings
  
  //NB: Double-dispatch pattern, to avoid repeated pattern-matches in SlickLoamDao.
  def insertOrUpdate(tables: Tables): tables.driver.api.DBIO[Int]
}

object SettingRow {
  def fromSettings(settings: Settings, executionId: Int): SettingRow = settings match {
    case LocalSettings() => LocalSettingRow(executionId)
    case UgerSettings(mem, cpu, queue) => UgerSettingRow(executionId, mem, cpu, queue.name)
    case GoogleSettings(cluster) => GoogleSettingRow(executionId, cluster)
  }
}

final case class LocalSettingRow(executionId: Int) extends SettingRow {
  
  override def toSettings: Settings = LocalSettings()
  
  override def insertOrUpdate(tables: Tables): tables.driver.api.DBIO[Int] = {
    import tables.driver.api._
    
    tables.localSettings.insertOrUpdate(this)
  }
}

object LocalSettingRow {
  //NB: Since LocalSettingRow now takes 1 param, this object now implicitly extends
  //Function1, which doesn't have `tupled`.  This is lame, since 1-tuples exist, 
  //but oh well. :\
  //NB: We need tupled to make defining `*` projection easier in Tables.LocalSettings.
  def tupled: (Int) => LocalSettingRow = {
    case (eid) => LocalSettingRow(eid)
  }
}

final case class UgerSettingRow(executionId: Int,
                      mem: Int,
                      cpu: Int,
                      queue: String) extends SettingRow {

  //NB: TODO: .get :(
  override def toSettings: Settings = UgerSettings(mem, cpu, Queue.fromString(queue).get)
  
  override def insertOrUpdate(tables: Tables): tables.driver.api.DBIO[Int] = {
    import tables.driver.api._
    
    tables.ugerSettings.insertOrUpdate(this)
  }
}

final case class GoogleSettingRow(executionId: Int,
                                  cluster: String) extends SettingRow {

  override def toSettings: Settings = GoogleSettings(cluster)
  
  override def insertOrUpdate(tables: Tables): tables.driver.api.DBIO[Int] = {
    import tables.driver.api._
    
    tables.googleSettings.insertOrUpdate(this)
  }
}

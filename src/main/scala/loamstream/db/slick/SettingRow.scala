package loamstream.db.slick

import loamstream.conf.Settings
import loamstream.conf.GoogleSettings
import loamstream.conf.LocalSettings
import loamstream.conf.UgerSettings
import loamstream.uger.Queue
import slick.jdbc.JdbcProfile
import loamstream.model.quantities.Cpus
import loamstream.model.quantities.Memory
import loamstream.model.quantities.CpuTime

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
    case LocalSettings => LocalSettingRow(executionId)
    case UgerSettings(cpus, memPerCpu, maxRunTime, queue) => {
      UgerSettingRow(executionId, cpus.value, memPerCpu.gb, maxRunTime.hours, queue.name)
    }
    case GoogleSettings(cluster) => GoogleSettingRow(executionId, cluster)
  }
}

final case class LocalSettingRow(executionId: Int) extends SettingRow {
  
  override def toSettings: Settings = LocalSettings
  
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

final case class UgerSettingRow(
    executionId: Int,
    cpus: Int,
    //TODO: Make units explicit for memPerCpu and maxRunTime 
    memPerCpu: Double,  //in GB
    maxRunTime: Double, //in hours
    queue: String) extends SettingRow {

  //NB: TODO: .get :(
  override def toSettings: Settings = {
    UgerSettings(Cpus(cpus), Memory.inGb(memPerCpu), CpuTime.inHours(maxRunTime), Queue.fromString(queue).get)
  }
  
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

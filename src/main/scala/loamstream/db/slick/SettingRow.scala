package loamstream.db.slick

import loamstream.model.execute.GoogleSettings
import loamstream.model.execute.LocalSettings
import loamstream.model.execute.Settings
import loamstream.model.quantities.CpuTime
import loamstream.model.quantities.Cpus
import loamstream.model.quantities.Memory
import loamstream.drm.Queue
import loamstream.model.execute.DrmSettings
import loamstream.model.execute.Environment

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
  def fromEnvironment(environment: Environment, executionId: Int): SettingRow = environment match {
    case Environment.Local => LocalSettingRow(executionId)
    case Environment.Uger(drmSettings) => UgerSettingRow.fromSettings(executionId, drmSettings)
    case Environment.Lsf(drmSettings) => LsfSettingRow.fromSettings(executionId, drmSettings)
    case Environment.Google(GoogleSettings(cluster)) => GoogleSettingRow(executionId, cluster)
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

protected trait DrmSettingRow extends SettingRow {
  def executionId: Int
  def cpus: Int
  //TODO: Make units explicit for memPerCpu and maxRunTime 
  def memPerCpu: Double  //in GB
  def maxRunTime: Double //in hours
  def queue: Option[String]
  
  final override def toSettings: Settings = {
    DrmSettings(Cpus(cpus), Memory.inGb(memPerCpu), CpuTime.inHours(maxRunTime), queue.map(Queue(_)))
  }
}

protected abstract class DrmSettingRowCompanion[R <: DrmSettingRow](
    make: (Int, Int, Double, Double, Option[String]) => R) extends ((Int, Int, Double, Double, Option[String]) => R) {
  
  def fromSettings(executionId: Int, settings: DrmSettings): R = {
    make(
        executionId, 
        settings.cores.value, 
        settings.memoryPerCore.gb, 
        settings.maxRunTime.hours, 
        settings.queue.map(_.name))
  }
}

final case class UgerSettingRow(
    executionId: Int,
    cpus: Int,
    //TODO: Make units explicit for memPerCpu and maxRunTime 
    memPerCpu: Double,  //in GB
    maxRunTime: Double, //in hours
    queue: Option[String]) extends DrmSettingRow {

  override def insertOrUpdate(tables: Tables): tables.driver.api.DBIO[Int] = {
    import tables.driver.api._
    
    tables.ugerSettings.insertOrUpdate(this)
  }
}

object UgerSettingRow extends DrmSettingRowCompanion[UgerSettingRow](new UgerSettingRow(_, _, _, _, _))

final case class LsfSettingRow(
    executionId: Int,
    cpus: Int,
    //TODO: Make units explicit for memPerCpu and maxRunTime 
    memPerCpu: Double,  //in GB
    maxRunTime: Double, //in hours
    queue: Option[String]) extends DrmSettingRow {

  override def insertOrUpdate(tables: Tables): tables.driver.api.DBIO[Int] = {
    import tables.driver.api._
    
    tables.lsfSettings.insertOrUpdate(this)
  }
}

object LsfSettingRow extends DrmSettingRowCompanion[LsfSettingRow](new LsfSettingRow(_, _, _, _, _))

final case class GoogleSettingRow(executionId: Int,
                                  cluster: String) extends SettingRow {

  override def toSettings: Settings = GoogleSettings(cluster)
  
  override def insertOrUpdate(tables: Tables): tables.driver.api.DBIO[Int] = {
    import tables.driver.api._
    
    tables.googleSettings.insertOrUpdate(this)
  }
}

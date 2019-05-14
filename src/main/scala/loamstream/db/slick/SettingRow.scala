package loamstream.db.slick

import loamstream.model.execute.GoogleSettings
import loamstream.model.execute.LocalSettings
import loamstream.model.execute.Settings
import loamstream.model.quantities.CpuTime
import loamstream.model.quantities.Cpus
import loamstream.model.quantities.Memory
import loamstream.drm.Queue
import loamstream.model.execute.DrmSettings
import java.nio.file.Path
import loamstream.drm.ContainerParams
import loamstream.model.execute.UgerDrmSettings
import loamstream.model.execute.LsfDrmSettings
import loamstream.model.execute.EnvironmentType

/**
 * @author kyuksel
 *         date: 3/7/17
 */
sealed trait SettingRow {
  //NB: Double-dispatch pattern, to avoid repeated pattern-matches in SlickLoamDao.
  def insertOrUpdate(tables: Tables): tables.driver.api.DBIO[Int]
}

sealed trait HasSimpleToSettings {
  def toSettings: Settings
}

sealed trait HasContainerParamsToSettings {
  def toSettings(containerParamsOpt: Option[ContainerParams]): Settings
}

object SettingRow {
  def fromSettings(settings: Settings, executionId: Int): SettingRow = settings match {
    case LocalSettings => LocalSettingRow(executionId)
    case ugerSettings: UgerDrmSettings => UgerSettingRow.fromSettings(executionId, ugerSettings)
    case lsfSettings: LsfDrmSettings => LsfSettingRow.fromSettings(executionId, lsfSettings)
    case GoogleSettings(cluster) => GoogleSettingRow(executionId, cluster)
  }
}

final case class LocalSettingRow(executionId: Int) extends SettingRow with HasSimpleToSettings {
  
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
  def tupled: (Int) => LocalSettingRow = LocalSettingRow(_)
}

protected trait DrmSettingRow extends SettingRow {
  def executionId: Int
  def cpus: Int
  //TODO: Make units explicit for memPerCpu and maxRunTime 
  def memPerCpu: Double  //in GB
  def maxRunTime: Double //in hours
  def queue: Option[String]
  
  protected final def makeQueueOpt: Option[Queue] = queue.map(Queue(_))
  protected final def makeCpus: Cpus = Cpus(cpus)
  protected final def makeMemory: Memory = Memory.inGb(memPerCpu)
  protected final def makeCpuTime: CpuTime = CpuTime.inHours(maxRunTime)
}

protected object DrmSettingRow {
  type Maker[R <: DrmSettingRow] = (Int, Int, Double, Double, Option[String]) => R
}

protected abstract class DrmSettingRowCompanion[R <: DrmSettingRow](
    make: DrmSettingRow.Maker[R]) extends DrmSettingRow.Maker[R] {
  
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
    queue: Option[String]) extends DrmSettingRow with HasContainerParamsToSettings {

  override def toSettings(containerParamsOpt: Option[ContainerParams]): Settings = {
    UgerDrmSettings(makeCpus, makeMemory, makeCpuTime, makeQueueOpt, containerParamsOpt)
  }
  
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
    queue: Option[String]) extends DrmSettingRow with HasContainerParamsToSettings {

  override def toSettings(containerParamsOpt: Option[ContainerParams]): Settings = {
    LsfDrmSettings(makeCpus, makeMemory, makeCpuTime, makeQueueOpt, containerParamsOpt)
  }
  
  override def insertOrUpdate(tables: Tables): tables.driver.api.DBIO[Int] = {
    import tables.driver.api._
    
    tables.lsfSettings.insertOrUpdate(this)
  }
}

object LsfSettingRow extends DrmSettingRowCompanion[LsfSettingRow](new LsfSettingRow(_, _, _, _, _))

final case class GoogleSettingRow(executionId: Int,
                                  cluster: String) extends SettingRow with HasSimpleToSettings {

  override def toSettings: Settings = GoogleSettings(cluster)
  
  override def insertOrUpdate(tables: Tables): tables.driver.api.DBIO[Int] = {
    import tables.driver.api._
    
    tables.googleSettings.insertOrUpdate(this)
  }
}

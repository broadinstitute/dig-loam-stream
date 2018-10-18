package loamstream.db.slick

import loamstream.drm.ContainerParams
import loamstream.model.execute.DrmSettings
import loamstream.model.execute.LsfDrmSettings
import loamstream.model.execute.UgerDrmSettings

/**
 * @author clint
 * Jun 8, 2018
 */
sealed trait ContainerSettingsRow extends InsertOrUpdatable {
  def drmSettingsId: Int
  
  def imageName: String

  def toContainerParams: ContainerParams = ContainerParams(imageName)
}

sealed trait ContainerSettingsRowCompanion[R] extends ((Int, String) => R) {
  def unapply(r: R): Option[(Int, String)]
  
  def slickMappingTuple: (((Int, String)) => R, R => Option[(Int, String)]) = {
    ((this.apply(_, _)).tupled, this.unapply(_))
  }
  
  def fromContainerParams(drmSettingsId: Int, containerParams: ContainerParams): R = {
    this.apply(drmSettingsId, containerParams.imageName)
  }
}

object ContainerSettingsRowCompanion {
  def fromContainerParams(drmSettingsId: Int, drmSettings: DrmSettings): Option[ContainerSettingsRow] = {
    drmSettings match {
      case LsfDrmSettings(_, _, _, _, Some(params)) => {
        Some(LsfContainerSettingsRow.fromContainerParams(drmSettingsId, params))
      }
      case UgerDrmSettings(_, _, _, _, Some(params)) => {
        Some(UgerContainerSettingsRow.fromContainerParams(drmSettingsId, params))
      }
      case _ => None
    }
  }
}

final case class LsfContainerSettingsRow(drmSettingsId: Int, imageName: String) extends ContainerSettingsRow {
  override def insertOrUpdate(tables: Tables): tables.driver.api.DBIO[Int] = {
    import tables.driver.api._
    
    tables.lsfContainerSettings.insertOrUpdate(this)
  }
}

object LsfContainerSettingsRow extends ContainerSettingsRowCompanion[LsfContainerSettingsRow]

final case class UgerContainerSettingsRow(drmSettingsId: Int, imageName: String) extends ContainerSettingsRow {
  override def insertOrUpdate(tables: Tables): tables.driver.api.DBIO[Int] = {
    import tables.driver.api._
    
    tables.ugerContainerSettings.insertOrUpdate(this)
  }
}

object UgerContainerSettingsRow extends ContainerSettingsRowCompanion[UgerContainerSettingsRow]

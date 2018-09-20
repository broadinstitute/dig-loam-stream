package loamstream.db.slick

import loamstream.drm.DockerParams

/**
 * @author clint
 * Jun 8, 2018
 */
trait DockerSettingsRow extends InsertOrUpdatable {
  def drmSettingsId: Int
  
  def imageName: String

  def toDockerParams: DockerParams
}

final case class LsfDockerSettingsRow(drmSettingsId: Int, imageName: String) extends DockerSettingsRow {
  
  override def toDockerParams: DockerParams = DockerParams(imageName)
  
  override def insertOrUpdate(tables: Tables): tables.driver.api.DBIO[Int] = {
    import tables.driver.api._
    
    tables.lsfDockerSettings.insertOrUpdate(this)
  }
}

object LsfDockerSettingsRow extends ((Int, String) => LsfDockerSettingsRow) {
  def fromDockerParams(drmSettingsId: Int, dockerParams: DockerParams): LsfDockerSettingsRow = {
    LsfDockerSettingsRow(drmSettingsId, dockerParams.imageName)
  }
}

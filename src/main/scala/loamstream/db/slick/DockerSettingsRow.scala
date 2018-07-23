package loamstream.db.slick

import java.nio.file.Path
import loamstream.drm.DockerParams
import loamstream.drm.lsf.LsfDockerParams
import java.nio.file.Paths
import loamstream.util.BashScript

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
  
  override def toDockerParams: DockerParams = LsfDockerParams(imageName)
  
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

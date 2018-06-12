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
  
  def outputDir: String
  
  def toDockerParams(mountedDirs: Iterable[Path]): DockerParams
}

final case class LsfDockerSettingsRow(
    drmSettingsId: Int, 
    imageName: String, 
    outputDir: String) extends DockerSettingsRow {
  
  override def toDockerParams(mountedDirs: Iterable[Path]): DockerParams = {
    import Paths.{get => toPath}
    
    LsfDockerParams(imageName, mountedDirs, toPath(outputDir))
  }
  
  override def insertOrUpdate(tables: Tables): tables.driver.api.DBIO[Int] = {
    import tables.driver.api._
    
    tables.lsfDockerSettings.insertOrUpdate(this)
  }
}

object LsfDockerSettingsRow extends ((Int, String, String) => LsfDockerSettingsRow) {
  def fromDockerParams(drmSettingsId: Int, dockerParams: DockerParams): LsfDockerSettingsRow = {
    import BashScript.Implicits._
    
    LsfDockerSettingsRow(drmSettingsId, dockerParams.imageName, dockerParams.outputDir.toAbsolutePath.render)
  }
}

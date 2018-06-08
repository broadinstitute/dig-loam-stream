package loamstream.db.slick

import java.nio.file.Path
import loamstream.drm.DockerParams
import loamstream.drm.lsf.LsfDockerParams
import java.nio.file.Paths

/**
 * @author clint
 * Jun 8, 2018
 */
trait DockerSettingsRow {
  def drmSettingsId: Int
  
  def imageName: String
  
  def outputDir: String
  
  def toDockerParams(mountedDirs: Iterable[Path]): DockerParams
}

final case class LsfDockerSettingsRow(drmSettingsId: Int, imageName: String, outputDir: String) {
  def toDockerParams(mountedDirs: Iterable[Path]): DockerParams = {
    import Paths.{get => toPath}
    
    LsfDockerParams(imageName, mountedDirs, toPath(outputDir))
  }
}

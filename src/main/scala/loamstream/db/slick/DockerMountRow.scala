package loamstream.db.slick

import java.nio.file.Path
import java.nio.file.Paths
import loamstream.util.BashScript

/**
 * @author clint
 * Jun 8, 2018
 */
trait DockerMountRow extends InsertOrUpdatable {
  def drmSettingsId: Int
  
  def mountedDir: String
  
  final def path: Path = Paths.get(mountedDir)
}

final case class LsfDockerMountRow(id: Int, drmSettingsId: Int, mountedDir: String) extends DockerMountRow {
  override def insertOrUpdate(tables: Tables): tables.driver.api.DBIO[Int] = {
    import tables.driver.api._
    
    tables.lsfDockerMounts.insertOrUpdate(this)
  }
}

object LsfDockerMountRow extends ((Int, Int, String) => LsfDockerMountRow) {
  def fromPath(drmSettingsId: Int)(mountedDir: Path): DockerMountRow = {
    import BashScript.Implicits._
    
    LsfDockerMountRow(Helpers.dummyId, drmSettingsId, mountedDir.toAbsolutePath.render)
  }
}


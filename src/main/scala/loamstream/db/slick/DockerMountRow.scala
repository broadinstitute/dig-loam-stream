package loamstream.db.slick

import java.nio.file.Path
import java.nio.file.Paths

/**
 * @author clint
 * Jun 8, 2018
 */
final case class DockerMountRow(drmSettingsId: Int, mountedDir: String) {
  def path: Path = Paths.get(mountedDir)
}

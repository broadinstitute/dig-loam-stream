package loamstream.conf

import java.nio.file.Path

/**
 * @author clint
 * Jun 13, 2017
 */
trait HasScriptDir {
  def scriptDir: Path
}

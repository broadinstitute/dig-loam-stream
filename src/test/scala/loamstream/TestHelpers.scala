package loamstream

import java.nio.file.Paths
import java.nio.file.Path

/**
 * @author clint
 * date: Mar 10, 2016
 */
object TestHelpers {
  def path(p: String): Path = Paths.get(p)
}
package loamstream.db.slick

import java.nio.file.Path

/**
 * @author clint
 * date: Aug 10, 2016
 */
object Helpers {
  def normalize(p: Path): String = p.toAbsolutePath.toString
}
package loamstream.db.slick

import java.nio.file.Path
import java.sql.Timestamp
import java.time.Instant

/**
 * @author clint
 * date: Aug 10, 2016
 */
object Helpers {
  def normalize(p: Path): String = p.toAbsolutePath.toString
  
  def timestampFromLong(millisSinceEpoch: Long): Timestamp = {
    Timestamp.from(Instant.ofEpochMilli(millisSinceEpoch))
  }
}
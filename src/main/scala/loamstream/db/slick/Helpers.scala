package loamstream.db.slick

import java.nio.file.Path
import java.sql.Timestamp
import java.time.Instant

/**
 * @author clint
 * date: Aug 10, 2016
 */
object Helpers {
  def timestampFromLong(millisSinceEpoch: Long): Timestamp = {
    Timestamp.from(Instant.ofEpochMilli(millisSinceEpoch))
  }
  
  val dummyId: Int = -1
}
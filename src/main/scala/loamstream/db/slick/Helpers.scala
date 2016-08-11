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
  
  def lastModifiedTime(p: Path): Instant = {
    val file = p.toFile
    
    val lastModifiedTimeInMillis = if(file.exists) file.lastModified else 0L
    
    Instant.ofEpochMilli(lastModifiedTimeInMillis)
  }
  
  def timestampFromLong(millisSinceEpoch: Long): Timestamp = {
    Timestamp.from(Instant.ofEpochMilli(millisSinceEpoch))
  }
}
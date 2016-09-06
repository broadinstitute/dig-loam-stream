package loamstream.db.slick

import loamstream.util.Sequence
import java.nio.file.Paths
import java.nio.file.Files
import java.nio.file.Path

/**
 * @author clint
 * date: Aug 12, 2016
 */
object TestDbDescriptors {
  //TODO: This shouldn't be necessary :(
  private val sequence: Sequence[Int] = Sequence()

  private def makeUrl(dbName: String): String = s"jdbc:h2:$dbName;DB_CLOSE_DELAY=-1"
  
  //TODO: Unique DB names shouldn't be necessary :(
  def inMemoryH2: DbDescriptor = DbDescriptor(DbType.H2, makeUrl(s"mem:test${sequence.next()}"))
}
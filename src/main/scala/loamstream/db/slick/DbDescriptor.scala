package loamstream.db.slick

import java.nio.file.Path
import java.nio.file.Paths
import loamstream.util.Sequence

/**
 * @author clint
 * date: Aug 10, 2016
 */
final case class DbDescriptor(dbType: DbType, url: String)

object DbDescriptor {
  def inMemory: DbDescriptor = DbDescriptor(DbType.H2, makeUrl(s"test${sequence.next()}"))
  
  //DB files live named .loamstream/db/loamstream.*, all under .loamstream/db
  val onDiskDefault: DbDescriptor = onDiskAt(Paths.get("./.loamstream/db/loamstream"))
  
  def onDiskAt(dbDir: Path): DbDescriptor = DbDescriptor(DbType.H2, s"jdbc:h2:${dbDir}")
  
  //TODO: This shouldn't be necessary :(
  private val sequence: Sequence[Int] = Sequence()

  private def makeUrl(dbName: String): String = s"jdbc:h2:mem:$dbName;DB_CLOSE_DELAY=-1"
}

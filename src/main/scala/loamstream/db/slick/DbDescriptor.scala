package loamstream.db.slick

import java.nio.file.Path
import java.nio.file.Paths
import loamstream.util.Sequence
import loamstream.conf.Locations

/**
 * @author clint
 * date: Aug 10, 2016
 */
sealed trait DbDescriptor {
  def dbType: DbType
  def url: String
}

object DbDescriptor {
  def apply(dbType: DbType, url: String): DbDescriptor = TypeAndJdbcUrl(dbType, url)
  
  final case class TypeAndJdbcUrl(dbType: DbType, url: String) extends DbDescriptor
  
  final case class OnDiskH2(dbDir: Path, dbName: String) extends DbDescriptor {
    override val dbType: DbType = DbType.H2
    
    override val url: String = s"jdbc:h2:${dbDir.resolve(dbName)}"
  }
  
  final case class OnDiskHsql(dbDir: Path, dbName: String) extends DbDescriptor {
    override val dbType: DbType = DbType.Hsqldb
    
    override val url: String = s"jdbc:hsqldb:${dbDir.resolve(dbName)}"
  }
  
  def inMemoryH2: DbDescriptor = TypeAndJdbcUrl(DbType.H2, makeH2Url(s"test${sequence.next()}"))
  
  def inMemoryHsqldb: DbDescriptor = TypeAndJdbcUrl(DbType.Hsqldb, makeHsqldbUrl(s"test${sequence.next()}"))

  //DB files live named .loamstream/db/loamstream.*, all under .loamstream/db
  val defaultDbName: String = "loamstream"
  
  def onDiskDefault: DbDescriptor = onDiskHsqldbAt(Locations.Default.dbDir, defaultDbName)
  
  def onDiskHsqldbAt(dbDir: Path, dbName: String): OnDiskHsql = OnDiskHsql(dbDir, dbName)
  
  //TODO: This shouldn't be necessary :(
  private val sequence: Sequence[Int] = Sequence()

  private def makeH2Url(dbName: String): String = s"jdbc:h2:mem:$dbName;DB_CLOSE_DELAY=-1"
  
  private def makeHsqldbUrl(dbName: String): String = s"jdbc:hsqldb:mem:$dbName"
}

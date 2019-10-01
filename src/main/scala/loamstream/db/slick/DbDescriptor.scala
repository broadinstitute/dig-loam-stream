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
  final case class TypeAndJdbcUrl(dbType: DbType, url: String) extends DbDescriptor
  
  def inMemoryHsqldb: DbDescriptor = inMemoryHsqldb(defaultDbName)

  def inMemoryHsqldb(dbName: String): DbDescriptor = {
    TypeAndJdbcUrl(DbType.Hsqldb, makeHsqldbInMemUrl(s"${dbName}${sequence.next()}"))
  }

  //DB files live named .loamstream/db/loamstream.*, all under .loamstream/db
  val defaultDbName: String = "loamstream"
  
  def onDiskDefault: DbDescriptor = onDiskHsqldbAt(Locations.Default.dbDir, defaultDbName)
  
  def onDiskHsqldbAt(dbDir: Path, dbName: String): TypeAndJdbcUrl = {
    TypeAndJdbcUrl(DbType.Hsqldb, makeHsqldbOnDiskUrl(dbDir, dbName))
  }
  
  //TODO: This shouldn't be necessary :(
  private val sequence: Sequence[Int] = Sequence()
  
  private def makeHsqldbInMemUrl(dbName: String): String = s"jdbc:hsqldb:mem:$dbName"
  
  private def makeHsqldbOnDiskUrl(dbDir: Path, dbName: String): String = s"jdbc:hsqldb:file:${dbDir.resolve(dbName)}"
}

package loamstream.db.slick

import java.nio.file.Path

import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.duration.Duration

import loamstream.db.LoamDao
import loamstream.util.Hash
import slick.driver.JdbcProfile
import java.sql.ResultSet
import java.sql.Timestamp

/**
 * @author clint
 * date: Aug 8, 2016
 * 
 * Rough-draft LoamDao implementation backed by Slick 
 */
final class SlickLoamDao(val descriptor: DbDescriptor) extends LoamDao {
  val driver = descriptor.driver
  
  import driver.api._
  import SlickLoamDao.waitFor
  
  override def hashFor(path: Path): Hash = {
    val query = tables.outputs.filter(_.path === Helpers.normalize(path)).result.head
    
    val futureRow = db.run(query)
    
    //TODO: Re-evaluate
    waitFor(futureRow).toHash
  }
  
  override def storeHash(path: Path, hash: Hash): Unit = {
    val newRow = new RawOutputRow(path, hash)
    
    val action = (tables.outputs += newRow).transactionally
    
    //TODO: Re-evaluate
    waitFor(db.run(action))
  }
  
  private[slick] lazy val db = Database.forURL(descriptor.url, driver = descriptor.jdbcDriverClass)

  private[slick] lazy val tables = new SlickLoamDao.Tables(driver)
}

object SlickLoamDao {
  final class Tables(val driver: JdbcProfile) {
    import driver.api._
    
    final class Outputs(tag: Tag) extends Table[RawOutputRow](tag, "OUTPUTS") {
      def path = column[String]("PATH", O.PrimaryKey)
      def lastModified = column[Timestamp]("LAST_MODIFIED")
      def hash = column[String]("HASH")
      def hashType = column[String]("HASH_TYPE")
      def * = (path, lastModified, hash, hashType) <> (RawOutputRow.tupled, RawOutputRow.unapply)
    }
    
    val outputs = TableQuery[Outputs]
    
    private def ddlForAllTables = outputs.schema
    
    def create(database: Database): Unit = perform(database)(ddlForAllTables.create)
    
    def drop(database: Database): Unit = perform(database)(ddlForAllTables.drop)
  
    private def perform(database: Database)(action: DBIO[_]): Unit = {
      waitFor(database.run(ddlForAllTables.create))
    }
  }
  
  //TODO: Re-evaluate; don't wait forever
  private def waitFor[A](f: Future[A]): A = Await.result(f, Duration.Inf)
}
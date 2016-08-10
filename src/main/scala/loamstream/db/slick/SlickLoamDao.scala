package loamstream.db.slick

import java.nio.file.Path

import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.duration.Duration

import loamstream.db.LoamDao
import loamstream.util.Hash
import slick.driver.JdbcProfile
import java.sql.ResultSet

/**
 * @author clint
 * date: Aug 8, 2016
 * 
 * Rough-draft LoamDao implementation backed by Slick 
 */
final class SlickLoamDao(val descriptor: DbDescriptor) extends LoamDao {
  val driver = descriptor.driver
  
  import driver.api._
  
  override def hashFor(path: Path): Hash = {
    val query = tables.hashes.filter(_.path === Helpers.normalize(path)).result.head
    
    val futureRow = db.run(query)
    
    //TODO: Re-evaluate
    waitFor(futureRow).toHash
  }
  
  override def storeHash(path: Path, hash: Hash): Unit = {
    val newRow = new HashRow(path, hash)
    
    val action = DBIO.seq(tables.hashes += newRow).transactionally
    
    //TODO: Re-evaluate
    waitFor(db.run(action))
  }
  
  private[slick] lazy val db = Database.forURL(descriptor.url, driver = descriptor.jdbcDriverClass)

  private[slick] lazy val tables = new SlickLoamDao.Tables(driver)
  
  //TODO: Re-evaluate; don't wait forever
  private def waitFor[A](f: Future[A]): A = Await.result(f, Duration.Inf)
}

object SlickLoamDao {
  final class Tables(val driver: JdbcProfile) {
    import driver.api._
    
    final class Hashes(tag: Tag) extends Table[HashRow](tag, "HASHES") {
      def path = column[String]("PATH", O.PrimaryKey)
      def hash = column[String]("HASH")
      def hashType = column[String]("HASH_TYPE")
      def * = (path, hash, hashType) <> (HashRow.tupled, HashRow.unapply)
    }
    
    val hashes = TableQuery[Hashes]
    
    private def ddlForAllTables = hashes.schema
    
    def create(database: Database): Unit = perform(database)(ddlForAllTables.create)
    
    def drop(database: Database): Unit = perform(database)(ddlForAllTables.drop)
    
    private def perform(database: Database)(action: DBIO[_]): Unit = {
      val f = database.run(ddlForAllTables.create)
      
      Await.result(f, Duration.Inf)
    }
  }
}
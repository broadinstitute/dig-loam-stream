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
import loamstream.model.jobs.Output.CachedOutput
import scala.util.Try

/**
 * @author clint
 * date: Aug 8, 2016
 * 
 * Rough-draft LoamDao implementation backed by Slick 
 */
final class SlickLoamDao(val descriptor: DbDescriptor) extends LoamDao {
  val driver = descriptor.dbType.driver
  
  import driver.api._
  import SlickLoamDao.waitFor
  
  override def hashFor(path: Path): Hash = {
    val normalizedPath = Helpers.normalize(path)
    
    val query = tables.outputs.filter(_.path === normalizedPath).result.head.transactionally
    
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
  
  private def deleteAction(pathsToDelete: Iterable[Path]): DBIO[Int] = {
    val toDelete = pathsToDelete.map(Helpers.normalize).toSet
    
    tables.outputs.filter(_.path.inSetBind(toDelete)).delete
  }
  
  override def delete(paths: Iterable[Path]): Unit = {
    val action = deleteAction(paths).transactionally
    
    waitFor(db.run(action))
  }
  
  override def insertOrUpdate(rows: Iterable[CachedOutput]): Unit = {
    val paths = rows.map(_.path)
    
    val rawRows = rows.map(row => new RawOutputRow(row.path, row.hash))

    val insertAction = tables.outputs ++= rawRows
    
    val action = DBIO.seq(deleteAction(paths), insertAction).transactionally
    
    //TODO: Re-evaluate
    waitFor(db.run(action))
  }
  
  override def allRows: Seq[CachedOutput] = {
    val query = tables.outputs.result.transactionally
    
    val futureRow = db.run(query)
    
    //TODO: Re-evaluate
    waitFor(futureRow).map(_.toCachedOutput)
  }
  
  override def createTables(): Unit = tables.create(db)
  
  override def dropTables(): Unit = tables.drop(db)
  
  override def shutdown(): Unit = waitFor(db.shutdown)
  
  private[slick] lazy val db = Database.forURL(descriptor.url, driver = descriptor.dbType.jdbcDriverClass)

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
      def * = (path, lastModified, hash, hashType) <> (RawOutputRow.tupled, RawOutputRow.unapply) //scalastyle:ignore
    }
    
    val outputs = TableQuery[Outputs]
    
    private def ddlForAllTables = outputs.schema
    
    def create(database: Database): Unit = perform(database)(ddlForAllTables.create.transactionally)
    
    def drop(database: Database): Unit = perform(database)(ddlForAllTables.drop.transactionally)
  
    private def perform(database: Database)(action: DBIO[_]): Unit = {
      waitFor(database.run(action))
    }
  }
  
  //TODO: Re-evaluate; don't wait forever
  private def waitFor[A](f: Future[A]): A = Await.result(f, Duration.Inf)
}
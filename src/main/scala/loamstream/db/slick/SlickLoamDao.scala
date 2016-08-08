package loamstream.db.slick

import java.nio.file.Path

import scala.concurrent.Await
import scala.concurrent.duration.Duration

import loamstream.db.LoamDao
import loamstream.util.Hash

import slick.driver.JdbcProfile

/**
 * @author clint
 * date: Aug 8, 2016
 * 
 * Rough-draft LoamDao implementation backed by Slick 
 */
final class SlickLoamDao(driver: JdbcProfile) extends LoamDao {
  import driver.api._
  
  override def hashFor(path: Path): Hash = {
    val query = tables.hashes.filter(_.path === path.toString).result.head
    
    val futureRow = db.run(query)
    
    //TODO: Re-evaluate
    val row = Await.result(futureRow, Duration.Inf)

    row.toHash
  }
  
  override def storeHash(path: Path, hash: Hash): Unit = {
    val newRow = new HashRow(path, hash)
    
    val future = db.run(tables.hashes.insertOrUpdate(newRow))
    
    //TODO: Re-evaluate
    Await.result(future, Duration.Inf)
  }
  
  //TODO
  private lazy val db = Database.forURL("jdbc:h2:mem:hello", driver = "org.h2.Driver")
  
  private lazy val tables = new SlickLoamDao.Tables(driver) 
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
  }
}
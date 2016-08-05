package loamstream.db.slick

import slick.driver.JdbcProfile
import loamstream.db.LoamDao
import java.nio.file.Path
import loamstream.util.Hash
import scala.concurrent.Await
import scala.concurrent.duration.Duration

final class SlickLoamDao(driver: JdbcProfile) extends LoamDao {
  import driver.api._
  
  override def hashFor(path: Path): Hash = {
    val query = tables.hashes.filter(_.path === path.toString).result.head
    
    val futureRow = db.run(query)
    
    val row = Await.result(futureRow, Duration.Inf)

    row.toHash
  }
  
  //TODO
  private lazy val db = Database.forURL("jdbc:h2:mem:hello", driver = "org.h2.Driver")
  
  private lazy val tables = new SlickLoamDao.Tables(driver) 
}

object SlickLoamDao {
  /*final class Tables(val driver: JdbcProfile) {
    import driver.api._
    
    final class Hashes(tag: Tag) extends Table[(String, String, String)](tag, "HASHES") {
      def path = column[String]("PATH", O.PrimaryKey)
      def hash = column[String]("HASH")
      def hashType = column[String]("HASH_TYPE")
      def * = (path, hash, hashType)
    }
    
    val hashes = TableQuery[Hashes]
  }*/
  
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
  
  /*class Suppliers(tag: Tag)
  extends Table[(Int, String, String, String, String, String)](tag, "SUPPLIERS") {

  // This is the primary key column:
  def id: Column[Int] = column[Int]("SUP_ID", O.PrimaryKey)
  def name: Column[String] = column[String]("SUP_NAME")
  def street: Column[String] = column[String]("STREET")
  def city: Column[String] = column[String]("CITY")
  def state: Column[String] = column[String]("STATE")
  def zip: Column[String] = column[String]("ZIP")
  
  // Every table needs a * projection with the same type as the table's type parameter
  def * : ProvenShape[(Int, String, String, String, String, String)] =
    (id, name, street, city, state, zip)
}
  
  import com.lightbend.slick._
  
  final class Hashes(tag: Tag) extends Table[(String, Double)](tag, "COFFEES") {
  def name = column[String]("COF_NAME", O.PrimaryKey)
  def price = column[Double]("PRICE")
  def * = (name, price)
}
val coffees = TableQuery[Coffees]*/
}
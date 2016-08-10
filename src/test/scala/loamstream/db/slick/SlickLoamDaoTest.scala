package loamstream.db.slick

import org.scalatest.FunSuite
import java.nio.file.Paths
import loamstream.util.Hashes
import scala.util.control.NonFatal
import scala.concurrent.duration.Duration
import scala.concurrent.Await

/**
 * @author clint
 * date: Aug 9, 2016
 */
final class SlickLoamDaoTest extends FunSuite {
  private val descriptor = DbDescriptor(slick.driver.H2Driver, "jdbc:h2:mem:test;DB_CLOSE_DELAY=-1", "org.h2.Driver")
  
  private val dao = new SlickLoamDao(descriptor)
  
  test("Insert/Read") {
    createTablesAndThen {
      val path = Paths.get("src/test/resources/for-hashing/foo.txt")

      dao.storeHash(path, Hashes.sha1(path))
    
      val hash = dao.hashFor(path)
    
    	assert(hash == Hashes.sha1(path))
    }
  }
  
  private def createTablesAndThen[A](f: => A): A = {
    def drop(): Unit = {
      try { dao.tables.drop(dao.db) }
      catch {
        case NonFatal(e) => ()
      }
    }
    
    def create(): Unit = dao.tables.create(dao.db)
    
    try {
      create()
      
      f
    } finally {
      drop()
    }
  }
}
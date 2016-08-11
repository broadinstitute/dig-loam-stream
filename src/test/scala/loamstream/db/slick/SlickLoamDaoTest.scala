package loamstream.db.slick

import org.scalatest.FunSuite
import java.nio.file.Paths
import loamstream.util.Hashes
import scala.util.control.NonFatal
import scala.concurrent.duration.Duration
import scala.concurrent.Await
import loamstream.db.OutputRow
import loamstream.util.Hash
import java.nio.file.Path
import scala.util.Try

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
  
  test("Insert/allRows") {
    createTablesAndThen {
      val path0 = Paths.get("src/test/resources/for-hashing/foo.txt")
      val path1 = Paths.get("src/test/resources/for-hashing/bigger")
      val path2 = Paths.get("src/test/resources/for-hashing/empty.txt")

      assert(dao.allRows.isEmpty)
      
      val hash0 = Hashes.sha1(path0)
      val hash1 = Hashes.sha1(path1)
      val hash2 = Hashes.sha1(path2)
      
      dao.storeHash(path0, hash0)
      dao.storeHash(path1, hash1)
      dao.storeHash(path2, hash2)
    
      def outputRow(p: Path, h: Hash): OutputRow = (new RawOutputRow(p, h)).toOutputRow
      
      val expected = Set(outputRow(path0, hash0), outputRow(path1, hash1), outputRow(path2, hash2)) 
    
      //Use Sets to ignore order
    	assert(dao.allRows.toSet == expected)
    }
  }
  
  private def createTablesAndThen[A](f: => A): A = {
    def drop(): Unit = dao.tables.drop(dao.db)
    
    def create(): Unit = dao.tables.create(dao.db)

    Try(drop())
      
    create()
      
    f
  }
}
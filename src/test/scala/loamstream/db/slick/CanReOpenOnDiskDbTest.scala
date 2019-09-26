package loamstream.db.slick

import java.nio.file.Path

import org.scalatest.FunSuite

import loamstream.TestHelpers

/**
 * @author clint
 * Sep 26, 2019
 */
final class CanReOpenOnDiskDbTest extends FunSuite {
  private val testName = getClass.getSimpleName
  
  test("Can re-open the same on-disk DB") {
    TestHelpers.withWorkDir(testName) { workDir1 =>
      
      def startAndStopIn(workDir: Path): Unit = {
        val hasDao0 = new CanReOpenOnDiskDbTest.HasDao(workDir, testName)
      
        hasDao0.startUp()
      
        hasDao0.shutDown()
      }
      
      startAndStopIn(workDir1)
      startAndStopIn(workDir1)
      
      TestHelpers.withWorkDir(testName) { workDir2 =>
        assert(workDir1 !== workDir2) 

        startAndStopIn(workDir2)
      }
    }
  }
}

object CanReOpenOnDiskDbTest {
  final class HasDao(dbDir: Path, dbName: String) extends ProvidesSlickLoamDao {
    override protected val descriptor: DbDescriptor = DbDescriptor.onDiskHsqldbAt(dbDir, dbName)
    
    def startUp(): Unit = dao.createTables()
    
    def shutDown(): Unit = dao.shutdown()
  }
}

package loamstream.db.slick

import org.scalatest.FunSuite
import java.nio.file.Path
import loamstream.TestHelpers

final class CanReOpenOnDiskDbTest extends FunSuite {
  test("Can re-open the same on-disk DB") {
    final class HasDao(dbDir: Path, dbName: String) extends ProvidesSlickLoamDao {
      override protected val descriptor: DbDescriptor = DbDescriptor.onDiskHsqldbAt(dbDir, dbName)
      
      def startUp(): Unit = dao.createTables()
      
      def shutDown(): Unit = dao.shutdown()
    }
    
    val testName = getClass.getSimpleName
    
    TestHelpers.withWorkDir(testName) { workDir1 =>
      val hasDao0 = new HasDao(workDir1, testName)
      
      hasDao0.startUp()
      
      hasDao0.shutDown()
      
      val hasDao1 = new HasDao(workDir1, testName)
      
      hasDao1.startUp()
      
      hasDao1.shutDown()
      
      TestHelpers.withWorkDir(testName) { workDir2 =>
        assert(workDir1 !== workDir2) 
        
        val hasDao0 = new HasDao(workDir2, testName)
        
        hasDao0.startUp()
        
        hasDao0.shutDown()
      }
    }
  }
}

package loamstream.db.slick

import org.scalatest.FunSuite
import java.nio.file.Path
import loamstream.TestHelpers

final class CanReOpenOnDiskDbTest extends FunSuite {
  test("Can re-open the same on-disk DB") {
    final class HasDao(dbDir: Path, dbName: String) extends ProvidesSlickLoamDao {
      override protected val descriptor: DbDescriptor = DbDescriptor.onDiskHsqldbAt(dbDir, dbName)
      
      def startUp(): Unit = {
        println(s"@@@@@@@ Starting up db '$dbDir'/'$dbName'")
        
        dao.createTables()
      }
      
      def shutDown(): Unit = {
        println(s"@@@@@@@ Shutting down db '$dbDir'/'$dbName'")
        
        dao.shutdown()
      }
    }
    
    val testName = getClass.getSimpleName
    
    TestHelpers.withWorkDir(testName) { workDir =>
      val hasDao0 = new HasDao(workDir, testName)
      
      hasDao0.startUp()
      
      hasDao0.shutDown()
      
      val hasDao1 = new HasDao(workDir, testName)
      
      hasDao1.startUp()
      
      hasDao1.shutDown()
    }
  }
}

package loamstream.util

import org.scalatest.FunSuite
import loamstream.TestHelpers
import java.nio.file.{ Files => JFiles }
import scala.concurrent.ExecutionContext
import better.files.{ File => BetterFile }

/**
 * @author clint
 * Dec 21, 2017
 */
final class FileWatchersTest extends FunSuite {
  
  import PathEnrichments._
  
  test("Waiting for a file that eventually appears works") {
    val workDir = TestHelpers.getWorkDir(getClass.getSimpleName)
    
    val initiallyNonExistantFile = workDir / "foo"
    
    assert(JFiles.exists(initiallyNonExistantFile) === false)
    
    import scala.concurrent.duration._ 
    
    val howLong = 5.seconds
    
    import ExecutionContext.Implicits.global
    
    val future = FileWatchers.waitForCreationOf(initiallyNonExistantFile, howLong)
    
    assert(JFiles.exists(initiallyNonExistantFile) === false)
    
    runInNewThread {
      Thread.sleep(1000)
      
      BetterFile(initiallyNonExistantFile).createIfNotExists()
    }
    
    Futures.waitFor(future)
    
    assert(JFiles.exists(initiallyNonExistantFile))
  }
  
  private def runInNewThread[A](f: => A): Thread = {
    val t = new Thread(new Runnable {
      override def run(): Unit = f
    })
    
    t.start()
    
    t
  }
  
  test("Waiting for a file that never appears times out") {
    val workDir = TestHelpers.getWorkDir(getClass.getSimpleName)
    
    val initiallyNonExistantFile = workDir / "foo"
    
    assert(JFiles.exists(initiallyNonExistantFile) === false)
    
    import scala.concurrent.duration._ 
    
    val howLong = 1.seconds
    
    import ExecutionContext.Implicits.global
    
    val future = FileWatchers.waitForCreationOf(initiallyNonExistantFile, howLong)
    
    assert(JFiles.exists(initiallyNonExistantFile) === false)
    
    intercept[Exception] {
      Futures.waitFor(future)
    }
    
    assert(JFiles.exists(initiallyNonExistantFile) === false)
  }
  
  test("In") {
    fail()
  }
  
  test("Resources are properly closed") {
    fail()
  }
}

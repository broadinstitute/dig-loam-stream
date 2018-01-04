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
      Thread.sleep(100)
      
      BetterFile(initiallyNonExistantFile).createIfNotExists()
    }
    
    Futures.waitFor(future)
    
    assert(JFiles.exists(initiallyNonExistantFile))
  }
  
  private def runInNewThread(f: => Any): Thread = {
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
    
    val howLong = 0.1.seconds
    
    import ExecutionContext.Implicits.global
    
    val future = FileWatchers.waitForCreationOf(initiallyNonExistantFile, howLong)
    
    assert(JFiles.exists(initiallyNonExistantFile) === false)
    
    intercept[Exception] {
      Futures.waitFor(future)
    }
    
    assert(JFiles.exists(initiallyNonExistantFile) === false)
  }
  
  test("In") {
    import scala.concurrent.duration._
    
    val howLong = 200.milliseconds
    
    @volatile var end = 0L
    
    val start = System.currentTimeMillis
    
    import ExecutionContext.Implicits.global
    
    val (terminable, future) = FileWatchers.in(howLong) {
      end = System.currentTimeMillis
    }
    
    Futures.waitFor(future)
    
    val elapsed = end - start
    
    assert(elapsed >= howLong.toMillis)
  }
  
  test("In - early cancellation") {
    import scala.concurrent.duration._
    
    val howLong = 0.5.seconds
    
    @volatile var end = 0L
    
    val start = System.currentTimeMillis
    
    import ExecutionContext.Implicits.global
    
    val (terminable, future) = FileWatchers.in(howLong) {
      end = System.currentTimeMillis
    }
    
    terminable.stop()
    
    intercept[Exception] {
      Futures.waitFor(future)
    }
    
    val elapsed = end - start
    
    assert(elapsed < howLong.toMillis)
  }
}

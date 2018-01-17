package loamstream.util

import org.scalatest.FunSuite
import loamstream.TestHelpers
import java.nio.file.{ Files => JFiles }
import scala.concurrent.ExecutionContext

/**
 * @author clint
 * Dec 21, 2017
 */
final class FileMonitorTest extends FunSuite {
  
  import PathEnrichments._
  import TestHelpers.waitFor
  
  test("Waiting for a file that eventually appears works") {
    val workDir = TestHelpers.getWorkDir(getClass.getSimpleName)
    
    val initiallyNonExistantFile = workDir / "foo"
    
    assert(JFiles.exists(initiallyNonExistantFile) === false)
    
    import scala.concurrent.duration._ 
    
    val howLong = 5.seconds
    
    val fileMonitor = new FileMonitor(pollingRateInHz = 10.0, howLong)
    
    val future = fileMonitor.waitForCreationOf(initiallyNonExistantFile)
    
    assert(JFiles.exists(initiallyNonExistantFile) === false)
    
    runInNewThread {
      Thread.sleep(100)
      
      if(!JFiles.exists(initiallyNonExistantFile)) {
        Files.writeTo(initiallyNonExistantFile)("")
      }
    }
    
    waitFor(future)
    
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
    
    val fileMonitor = new FileMonitor(pollingRateInHz = 10.0, howLong)
    
    val future = fileMonitor.waitForCreationOf(initiallyNonExistantFile)
    
    assert(JFiles.exists(initiallyNonExistantFile) === false)
    
    intercept[Exception] {
      waitFor(future)
    }
    
    assert(JFiles.exists(initiallyNonExistantFile) === false)
  }
}

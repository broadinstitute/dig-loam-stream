package loamstream.util

import org.scalatest.FunSuite
import loamstream.TestHelpers
import java.nio.file.{ Files => JFiles }
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.collection.compat._

/**
 * @author clint
 * Dec 21, 2017
 */
final class FileMonitorTest extends FunSuite {
  
  import Paths.Implicits._
  import TestHelpers.waitForT
  
  test("'Waiting' for a file that already exists") {
    val workDir = TestHelpers.getWorkDir(getClass.getSimpleName)
    
    val file = workDir / "foo"
    
    Files.writeTo(file)("blah blah blah")
    
    assert(JFiles.exists(file))
    
    import scala.concurrent.duration._ 
    
    val howLong = 5.seconds
    
    val fileMonitor = new FileMonitor(pollingRateInHz = 10.0, howLong)
    
    assert(fileMonitor.getWatchedFiles.isEmpty)
    
    val future = fileMonitor.waitForCreationOf(file)
    
    assert(future.isCompleted === true)
  }
  
  test("Waiting for a file that eventually appears") {
    val workDir = TestHelpers.getWorkDir(getClass.getSimpleName)
    
    val initiallyNonExistantFile = workDir / "foo"
    
    assert(JFiles.exists(initiallyNonExistantFile) === false)
    
    import scala.concurrent.duration._ 
    
    val howLong = 5.seconds
    
    val fileMonitor = new FileMonitor(pollingRateInHz = 10.0, howLong)
    
    assert(fileMonitor.getWatchedFiles.contains(initiallyNonExistantFile) === false)
    
    val future = fileMonitor.waitForCreationOf(initiallyNonExistantFile)
    
    assert(fileMonitor.getWatchedFiles.contains(initiallyNonExistantFile))
    
    assert(JFiles.exists(initiallyNonExistantFile) === false)
    
    runInNewThread {
      Thread.sleep(100)
      
      if(!JFiles.exists(initiallyNonExistantFile)) {
        Files.writeTo(initiallyNonExistantFile)("")
      }
    }
    
    TestHelpers.waitFor(future)
    
    assert(JFiles.exists(initiallyNonExistantFile))
    
    //Make sure we've cleaned up properly
    assert(fileMonitor.getWatchedFiles.contains(initiallyNonExistantFile) === false)
  }
  
  test("Waiting for a file that never appears times out") {
    val workDir = TestHelpers.getWorkDir(getClass.getSimpleName)
    
    val fileThatNeverAppears = workDir / "foo"
    
    assert(JFiles.exists(fileThatNeverAppears) === false)
    
    import scala.concurrent.duration._ 
    
    val howLong = 0.1.seconds
    
    val fileMonitor = new FileMonitor(pollingRateInHz = 10.0, howLong)
    
    assert(fileMonitor.getWatchedFiles.contains(fileThatNeverAppears) === false)
    
    val future = fileMonitor.waitForCreationOf(fileThatNeverAppears)
    
    assert(fileMonitor.getWatchedFiles.contains(fileThatNeverAppears))
    
    assert(JFiles.exists(fileThatNeverAppears) === false)
    
    intercept[Exception] {
      TestHelpers.waitFor(future)
    }
    
    //Make sure we've cleaned up properly
    assert(fileMonitor.getWatchedFiles.contains(fileThatNeverAppears) === false)
    
    assert(JFiles.exists(fileThatNeverAppears) === false)
  }
  
  test("Multiple watchers 'waiting' for a file that already exists") {
    val workDir = TestHelpers.getWorkDir(getClass.getSimpleName)
    
    val file = workDir / "foo"
    
    Files.writeTo(file)("blah blah blah")
    
    assert(JFiles.exists(file))
    
    import scala.concurrent.duration._ 
    
    val howLong = 5.seconds
    
    val fileMonitor = new FileMonitor(pollingRateInHz = 10.0, howLong)
    
    val futures = (1 to 5).map(_ => fileMonitor.waitForCreationOf(file))

    assert(fileMonitor.getWatchedFiles.isEmpty)
    
    assert(futures(0).isCompleted === true)
    assert(futures(1).isCompleted === true)
    assert(futures(2).isCompleted === true)
    assert(futures(3).isCompleted === true)
    assert(futures(4).isCompleted === true)
  }
  
  test("Multiple watchers waiting for a file that eventually appears") {
    val workDir = TestHelpers.getWorkDir(getClass.getSimpleName)
    
    val initiallyNonExistantFile = workDir / "foo"
    
    assert(JFiles.exists(initiallyNonExistantFile) === false)
    
    import scala.concurrent.duration._ 
    
    val howLong = 5.seconds
    
    val fileMonitor = new FileMonitor(pollingRateInHz = 10.0, howLong)
    
    assert(fileMonitor.getWatchedFiles.contains(initiallyNonExistantFile) === false)
    
    val futures = (1 to 5).map(_ => fileMonitor.waitForCreationOf(initiallyNonExistantFile))
    
    val watchers = fileMonitor.getWatchedFiles(initiallyNonExistantFile).to(Seq)
    
    assert(watchers.size === 5)
    
    assert(JFiles.exists(initiallyNonExistantFile) === false)
    
    runInNewThread {
      Thread.sleep(100)
      
      if(!JFiles.exists(initiallyNonExistantFile)) {
        Files.writeTo(initiallyNonExistantFile)("")
      }
    }
    
    import scala.concurrent.ExecutionContext.Implicits.global
    
    TestHelpers.waitFor(Future.sequence(futures))
    
    assert(JFiles.exists(initiallyNonExistantFile))
    
    //Make sure we've cleaned up properly
    assert(fileMonitor.getWatchedFiles.contains(initiallyNonExistantFile) === false)
    
    assert(watchers(0).isStopped)
    assert(watchers(1).isStopped)
    assert(watchers(2).isStopped)
    assert(watchers(3).isStopped)
    assert(watchers(4).isStopped)
  }
  
  test("Multiple watchers waiting for a file that never appears times out") { 
    val workDir = TestHelpers.getWorkDir(getClass.getSimpleName)
    
    val fileThatNeverAppears = workDir / "foo"
    
    assert(JFiles.exists(fileThatNeverAppears) === false)
    
    import scala.concurrent.duration._ 
    
    val howLong = 0.1.seconds
    
    val fileMonitor = new FileMonitor(pollingRateInHz = 10.0, howLong)
    
    assert(fileMonitor.getWatchedFiles.contains(fileThatNeverAppears) === false)
    
    val futures = (1 to 5).map(_ => fileMonitor.waitForCreationOf(fileThatNeverAppears))
    
    val watchers = fileMonitor.getWatchedFiles(fileThatNeverAppears).to(Seq)
    
    assert(watchers.size === 5)
    
    assert(JFiles.exists(fileThatNeverAppears) === false)
    
    import scala.concurrent.ExecutionContext.Implicits.global
    
    intercept[Exception] {
      TestHelpers.waitFor(Future.sequence(futures))
    }
    
    //Make sure we've cleaned up properly
    assert(fileMonitor.getWatchedFiles.contains(fileThatNeverAppears) === false)
    
    assert(JFiles.exists(fileThatNeverAppears) === false)
    
    assert(watchers(0).isStopped)
    assert(watchers(1).isStopped)
    assert(watchers(2).isStopped)
    assert(watchers(3).isStopped)
    assert(watchers(4).isStopped)
  }
  
  private def runInNewThread(f: => Any): Thread = {
    val t = new Thread(new Runnable {
      override def run(): Unit = f
    })
    
    t.start()
    
    t
  }
}

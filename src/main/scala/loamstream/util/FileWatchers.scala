package loamstream.util

import java.nio.file.Path
import java.nio.file.{ StandardWatchEventKinds => EventType }
import java.nio.file.WatchEvent
import java.util.Timer
import java.util.TimerTask

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.concurrent.duration.Duration
import scala.util.Success

import better.files.{ File => BetterFile }
import better.files.FileMonitor
import java.nio.file.{ Files => JFiles }
import java.util.concurrent.atomic.AtomicBoolean

/**
 * @author clint
 * Dec 21, 2017
 */
object FileWatchers extends Loggable {
  
  private[util] def hasParentDir(path: Path): Boolean = Option(path.getParent).isDefined
  
  def waitForCreationOf(file: Path, maxWaitTime: Duration)(implicit context: ExecutionContext): Future[Unit] = {
    val fileToWatch = normalize(file)
    
    if(JFiles.exists(fileToWatch)) {
      trace(s"NOT Waiting for $fileToWatch, since it already exists")
      
      Future.successful(())
    } else {
      require(hasParentDir(file), s"$file must have a parent directory")
      
      require(maxWaitTime.isFinite, "maxWaitTime can't be infinite")
      
      debug(s"Waiting for $fileToWatch")
      
      val promise: Promise[Unit] = Promise()
      
      val (timerHandle, _) = in(maxWaitTime) {
        promise.tryComplete(Tries.failure(s"Waited $maxWaitTime, but $fileToWatch was never created"))
      }

      val watcher: FileMonitor = new FileMonitor(file.getParent, recursive = false) {
        private def isCreationEvent(eventType: WatchEvent.Kind[Path]) = eventType == EventType.ENTRY_CREATE
        
        private def isFileWeCareAbout(f: BetterFile) = normalize(f) == fileToWatch  
        
        private def relevantFileWasCreated(et: WatchEvent.Kind[Path], f: BetterFile): Boolean = {
          isFileWeCareAbout(f) && isCreationEvent(et)
        }
        
        override def onEvent(eventType: WatchEvent.Kind[Path], eventFile: BetterFile, count: Int): Unit = {
          if(relevantFileWasCreated(eventType, eventFile)) {
            debug(s"$eventFile was created")
            
            stop()
            
            promise.tryComplete(Success(()))
          }
        }
      }
      
      watcher.start()
      
      def closeEverything(): Unit = {
        Throwables.quietly(s"Couldn't close file watcher for $fileToWatch")(watcher.stop())
        Throwables.quietly(s"Couldn't stop timer for $fileToWatch")(timerHandle.stop())
      }

      promise.future.map(_ => closeEverything())
    }
  }
  
  private[util] def in[A](howLong: Duration)(f: => A)(implicit context: ExecutionContext): (Terminable, Future[A]) = {
    val shouldRun: AtomicBoolean = new AtomicBoolean(true)

    //TODO: Find a better, cancelable way to wait.  For now, it's "ok" for one future to be blocked and 'in flight' 
    //for the duration specified by howLong.
    val future = Futures.runBlocking(Thread.sleep(howLong.toMillis)).flatMap { _ =>
      if(shouldRun.get) { Future(f) } 
      else { Future.failed(new Exception(s"Not running, since we were cancelled.")) }
    }
    
    val terminable = Terminable(shouldRun.set(false))
    
    (terminable, future) 
  }
  
  private def normalize(file: BetterFile): Path = PathUtils.normalizePath(file.toJava.toPath)
}

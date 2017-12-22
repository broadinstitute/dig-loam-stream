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

/**
 * @author clint
 * Dec 21, 2017
 */
object FileWatchers extends Loggable {
  
  private[util] def hasParentDir(path: Path): Boolean = Option(path.getParent).isDefined
  
  def waitForCreationOf(file: Path, maxWaitTime: Duration)(implicit context: ExecutionContext): Future[Unit] = {
    val fileToWatch = normalize(file)
    
    require(hasParentDir(file), s"$file must have a parent directory")
    
    require(maxWaitTime.isFinite, "maxWaitTime can't be infinite")
    
    debug(s"Waiting for $fileToWatch")
    
    def isFileWeCareAbout(f: BetterFile) = normalize(f) == fileToWatch
    
    val promise: Promise[Unit] = Promise()
    
    val watcher: FileMonitor = new FileMonitor(file.getParent, recursive = false) {
      override def onEvent(eventType: WatchEvent.Kind[Path], eventFile: BetterFile, count: Int): Unit = {
        if(isFileWeCareAbout(eventFile)) {
          if(eventType == EventType.ENTRY_CREATE) {
            debug(s"$eventFile was created")
            
            promise.tryComplete(Success(()))
          }
        }
      }
    }
    
    watcher.start()
    
    import Futures.Implicits._

    val timerHandle = in(maxWaitTime) {
      promise.tryComplete(Tries.failure(s"Waited $maxWaitTime, but $fileToWatch was never created"))
    }
    
    def closeEverything(): Unit = {
      Throwables.quietly(s"Couldn't close file watcher for $fileToWatch")(watcher.close())
      Throwables.quietly(s"Couldn't stop timer for $fileToWatch")(timerHandle.stop())
    }
    
    promise.future.withSideEffect(_ => closeEverything())
  }
  
  private[util] def in(howLong: Duration)(f: => Any): Terminable = {
    val timer = new Timer(true)
    
    def killTimer(): Unit = {
      timer.cancel() 
      timer.purge()
    }
    
    val task: TimerTask = new TimerTask {
      override def run(): Unit = { 
        try { f }
        finally { killTimer() }
      }
    }
    
    timer.schedule(task, howLong.toMillis)
    
    Terminable(killTimer())
  }
  
  private def normalize(file: BetterFile): Path = PathUtils.normalizePath(file.toJava.toPath)
}

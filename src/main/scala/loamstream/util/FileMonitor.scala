package loamstream.util

import java.util.Timer
import java.util.TimerTask
import java.nio.file.Path
import java.time.Instant
import java.nio.file.Files.exists
import java.util.concurrent.atomic.AtomicBoolean
import scala.concurrent.duration.Duration
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.util.Success

/**
 * @author clint
 * Jan 16, 2018
 */
final class FileMonitor(pollingRateInHz: Double, maxWaitTime: Duration) {
  import FileMonitor.Watcher
  
  def waitForCreationOf(path: Path): Future[Unit] = {
    val watcher = new Watcher(PathUtils.normalizePath(path), maxWaitTime, watchedFiles)
    
    watcher.future
  }
  
  private[this] val watchedFiles: ValueBox[FileMonitor.FilesToWatchers] = ValueBox(Map.empty)

  private val timer: Timer = {
    val timerIsDaemon = true

    val t = new Timer(timerIsDaemon)

    val periodInMillis = (1000.0 / pollingRateInHz).toLong

    val task: TimerTask = new FileMonitor.PollingTask(watchedFiles)

    t.scheduleAtFixedRate(task, 0, periodInMillis)

    t
  }
}

object FileMonitor {
  private type FilesToWatchers = Map[Path, Set[Watcher]]

  private final class PollingTask(watchedFiles: ValueBox[FilesToWatchers]) extends TimerTask {
    override def run(): Unit = {
      watchedFiles.foreach { filesAndWatchers =>
        handleCreatedFiles(filesAndWatchers)

        handleTimedOutWatchers(filesAndWatchers)
      }
    }

    private def timeExpired(now: Instant, watcher: Watcher): Boolean = now.isAfter(watcher.waitUntil)

    private def handleCreatedFiles(filesAndWatchers: FilesToWatchers): Unit = {
      val existingFilesAndWatchers = filesAndWatchers.iterator.filter { case (file, _) => exists(file) }

      for {
        (existingFile, watchers) <- existingFilesAndWatchers
        watcher <- watchers
      } {
        watcher.onPathCreation()
      }
    }

    private def handleTimedOutWatchers(filesAndWatchers: FilesToWatchers): Unit = {
      val now = Instant.now

      val tuples = filesAndWatchers.iterator.flatMap { case (file, watchers) => watchers.map(w => (file -> w)) }

      val timedOutFilesAndWatchers = tuples.filter { case (_, watcher) => timeExpired(now, watcher) }

      for {
        (file, timedOutWatcher) <- timedOutFilesAndWatchers
      } {
        timedOutWatcher.completeWithFailure {
          s"Timed out after ${timedOutWatcher.maxWaitTime} for '${file}' to appear"
        }
      }
    }
  }

  private final class Watcher(
      val fileToWatch: Path,
      val maxWaitTime: Duration,
      watchedFiles: ValueBox[FilesToWatchers]) extends Loggable {

    val waitUntil: Instant = Instant.now.plusMillis(maxWaitTime.toMillis)
    
    private[this] final val promise: Promise[Unit] = Promise()

    private val stopped: AtomicBoolean = new AtomicBoolean(true)

    private def isStopped: Boolean = stopped.get

    lazy val future: Future[Unit] = {
      start()

      promise.future
    }
    
    def stop(): Unit = watchedFiles.mutate { pathsToWatchers =>
      if (isStopped) {
        debug(s"Watcher for '$fileToWatch' already stopped.")
      }

      val newMap: FilesToWatchers = pathsToWatchers.get(fileToWatch) match {
        case Some(watchers) => {
          val newWatchers = watchers - this

          if (newWatchers.isEmpty) { pathsToWatchers - fileToWatch }
          else { pathsToWatchers.updated(fileToWatch, newWatchers) }
        }
        case None => {
          debug(s"We weren't - or are no longer - registered to watch file '$fileToWatch'.")

          pathsToWatchers
        }
      }

      stopped.set(true)

      completeWithFailure(s"Stopped waiting for '$fileToWatch'.", shouldStop = false)

      newMap
    }

    def start(): Unit = watchedFiles.mutate { pathsToWatchers =>
      if (isStopped) {
        val newMap: FilesToWatchers = pathsToWatchers.get(fileToWatch) match {
          case Some(watchers) => pathsToWatchers.updated(fileToWatch, watchers + this)
          case None           => pathsToWatchers + (fileToWatch -> Set(this))
        }

        stopped.set(false)

        newMap
      } else {
        pathsToWatchers
      }
    }
    
    def onPathCreation(): Unit = {
      promise.complete(Success(()))

      stop()
    }

    def completeWithFailure(message: String, shouldStop: Boolean = true): Unit = {
      promise.tryComplete(Tries.failure(message))
  
      if (shouldStop) {
        stop()
      }
    }
  }
}
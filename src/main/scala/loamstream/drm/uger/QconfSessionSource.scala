package loamstream.drm.uger

import loamstream.util.CommandInvoker
import loamstream.drm.SessionSource
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import loamstream.conf.UgerConfig
import rx.lang.scala.schedulers.IOScheduler
import rx.lang.scala.Scheduler
import loamstream.util.ValueBox
import loamstream.util.OneTimeLatch
import scala.util.control.NonFatal
import loamstream.util.Loggable
import scala.util.Try
import scala.util.Success
import scala.util.Failure

/**
 * @author clint
 * Jul 24, 2020
 */
final class QconfSessionSource(
    createInvoker: CommandInvoker.Sync[Unit],
    deleteInvoker: CommandInvoker.Sync[String])(implicit ec: ExecutionContext) extends SessionSource with Loggable {
  
  private[this] val sessionBox: ValueBox[Option[String]] = ValueBox(None)
  
  private[this] def doInit(): Unit = {
    val resultsAttempt = createInvoker.apply(())
    
    val sessionIdAttempt = resultsAttempt.map(_.stdout).flatMap(Qconf.parseOutput)
    
    sessionIdAttempt match {
      case Success(sessionId) => {
        sessionBox := Some(sessionId)
      
        debug(s"Created Uger session '$sessionId'")
      }
      case Failure(e) => {
        error(s"Couldn't get Uger session: ", e)
        //TODO
        throw e
      }
    }
  }

  override def isInitialized: Boolean = sessionBox.value.isDefined
  
  override def getSession: String = sessionBox.get { sessionIdOpt =>
    if(sessionIdOpt.isEmpty) {
      doInit()
    }
    
    sessionBox.value.getOrElse {
      sys.error(s"Uger session initialization failed previously; see earlier log messages")
    }
  }
  
  override def stop(): Unit = sessionBox.foreach { sessionIdOpt =>
    if(sessionIdOpt.isDefined) {
      try {
        deleteInvoker.apply(getSession)
      } catch {
        case NonFatal(e) => error(s"Couldn't delete Uger session") 
      }
    } else {
      debug("No Uger session initialized, not attempting to delete it")
    }
  }
}

object QconfSessionSource {
  def fromExecutable(
      ugerConfig: UgerConfig, 
      actualExecutable: String = "qconf",
      //TODO
      scheduler: Scheduler = IOScheduler())(implicit ec: ExecutionContext): QconfSessionSource = {
    
    val createInvoker= Qconf.createCommandInvoker(ugerConfig.maxRetries, actualExecutable, scheduler)
    
    val deleteInvoker = Qconf.deleteCommandInvoker(ugerConfig.maxRetries, actualExecutable, scheduler)
    
    new QconfSessionSource(createInvoker, deleteInvoker)
  }
}

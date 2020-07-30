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

/**
 * @author clint
 * Jul 24, 2020
 */
final class QconfSessionSource(
    createInvoker: CommandInvoker[Unit],
    deleteInvoker: CommandInvoker[String])(implicit ec: ExecutionContext) extends SessionSource with Loggable {
  
  private[this] val sessionBox: ValueBox[Option[String]] = ValueBox(None)
  
  private def isInitialized: Boolean = sessionBox.value.isDefined
  
  private[this] lazy val init: Unit = {
    val resultsFuture = createInvoker.apply(())
    
    val sessionIdFuture = resultsFuture.map(_.stdout).flatMap(lines => Future.fromTry(Qconf.parseOutput(lines)))
    
    //TODO
    try { 
      val sessionId = Await.result(sessionIdFuture, Duration.Inf)
      
      sessionBox := Some(sessionId)
    } catch {
      case NonFatal(e) => error(s"Couldn't get Uger session: ", e)
    }
  }
  
  private def doInit(): Unit = init
  
  override lazy val getSession: String = {
    doInit()
    
    sessionBox.value.getOrElse {
      sys.error(s"Uger session initialization failed previously; see earlier log messages")
    }
  }
  
  override def stop(): Unit = sessionBox.foreach { sessionIdOpt =>
    if(sessionIdOpt.isDefined) {
      val resultsFuture = deleteInvoker.apply(getSession)
    
      try {
        //TODO
        
        Await.result(resultsFuture, Duration.Inf)
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

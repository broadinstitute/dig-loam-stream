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

/**
 * @author clint
 * Jul 24, 2020
 */
final class QconfSessionSource(
    createInvoker: CommandInvoker[Unit],
    deleteInvoker: CommandInvoker[String])(implicit ec: ExecutionContext) extends SessionSource {
  
  override lazy val getSession: String = {
    val resultsFuture = createInvoker.apply(())
    
    val sessionIdFuture = resultsFuture.map(_.stdout).flatMap(lines => Future.fromTry(Qconf.parseOutput(lines)))
    
    //TODO
    Await.result(sessionIdFuture, Duration.Inf)
  }
  
  override def stop(): Unit = {
    val resultsFuture = deleteInvoker.apply(getSession)
    
    //TODO
    Await.result(resultsFuture, Duration.Inf)
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

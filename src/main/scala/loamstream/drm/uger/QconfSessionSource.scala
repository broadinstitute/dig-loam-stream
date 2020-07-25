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
final class QconfSessionSource(invoker: CommandInvoker[Unit])(implicit ec: ExecutionContext) extends SessionSource {
  override lazy val getSession: String = {
    val resultsFuture = invoker.apply(())
    
    val sessionIdFuture = resultsFuture.map(_.stdout).flatMap(lines => Future.fromTry(Qconf.parseOutput(lines)))
    
    //TODOs
    Await.result(sessionIdFuture, Duration.Inf)
  }
}

object QconfSessionCreator {
  def fromExecutable(
      ugerConfig: UgerConfig, 
      actualExecutable: String = "qconf",
      //TODO
      scheduler: Scheduler = IOScheduler())(implicit ec: ExecutionContext): QconfSessionSource = {
    
    new QconfSessionSource(Qconf.commandInvoker(ugerConfig.maxJobSubmissionRetries, actualExecutable, scheduler))
  }
}

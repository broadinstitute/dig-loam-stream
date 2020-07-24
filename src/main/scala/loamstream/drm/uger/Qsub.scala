package loamstream.drm.uger

import loamstream.util.Loggable
import scala.util.Try
import loamstream.util.RunResults
import scala.concurrent.ExecutionContext
import loamstream.util.CommandInvoker
import loamstream.conf.UgerConfig
import loamstream.drm.DrmTaskArray
import loamstream.model.execute.DrmSettings
import loamstream.util.Processes
import rx.lang.scala.schedulers.IOScheduler
import rx.lang.scala.Scheduler

/**
 * @author clint
 * Jul 24, 2020
 */
object Qsub extends Loggable {
  type InvocationFn[A] = A => Try[RunResults]
  
  final case class Params(ugerConfig: UgerConfig, settings: DrmSettings, taskArray: DrmTaskArray)
  
  private[uger] def makeTokens(actualExecutable: String, params: Params): Seq[String] = {
    import params.{ ugerConfig, settings, taskArray } 
    
    val staticPartFromUgerConfig = {
      ugerConfig.staticJobSubmissionParams.split("\\s+").iterator.map(_.trim).filter(_.nonEmpty)
    }

    val dynamicPart = {
      import settings._

      val numCores = cores.value
      val runTimeInHours: Int = maxRunTime.hours.toInt
      val mem: Int = memoryPerCore.gb.toInt

      val queuePart = queue.map(q => Seq("-q", q.name)).getOrElse(Nil)
      
      val osPart = containerParams match {
        case Some(_) => Seq("-l", "os=RedHat7")
        case None => Nil
      }
      
      val memPart = s"h_vmem=${mem}G"
      
      val runTimePart = s"h_rt=${runTimeInHours}:0:0"
      
      Seq(
        "-si",
        Sessions.sessionId,
        "-binding",
        s"linear:${numCores}",
        "-pe",
        "smp",
        numCores.toString) ++
      queuePart ++
      Seq(
        "-l",
        runTimePart,
        memPart) ++
      osPart :+ 
      taskArray.drmScriptFile.toAbsolutePath.toString
    }

    staticPartFromUgerConfig.toList ++ dynamicPart
  }
    
  final def commandInvoker(
      actualExecutable: String = "qsub",
      ugerConfig: UgerConfig,
      scheduler: Scheduler = IOScheduler())(implicit ec: ExecutionContext): CommandInvoker[Params] = {

    def invocationFn(params: Params): Try[RunResults] = {
      val tokens = makeTokens(actualExecutable, params)
      
      trace(s"Invoking '$actualExecutable': '${tokens.mkString(" ")}'")
      
      Processes.runSync(actualExecutable, tokens)
    }
    
    val justOnce = new CommandInvoker.JustOnce[Params](actualExecutable, invocationFn)
    
    new CommandInvoker.Retrying(justOnce, ugerConfig.maxJobSubmissionRetries, scheduler = scheduler)
  }
}

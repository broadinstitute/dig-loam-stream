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
import loamstream.drm.SessionSource
import java.nio.file.Path

/**
 * @author clint
 * Jul 24, 2020
 */
object Qsub extends Loggable {
  type InvocationFn[A] = A => Try[RunResults]
  
  final case class Params(ugerConfig: UgerConfig, settings: DrmSettings, taskArraySize: Int, drmScriptFile: Path)
  
  object Params {
    def apply(ugerConfig: UgerConfig, settings: DrmSettings, taskArray: DrmTaskArray): Params = {
      new Params(ugerConfig, settings, taskArray.size, taskArray.drmScriptFile)
    }
  }
  
  private[uger] def makeTokens(sessionSource: SessionSource, actualExecutable: String, params: Params): Seq[String] = {
    import params.{ ugerConfig, settings, taskArraySize, drmScriptFile } 
    
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
        sessionSource.getSession,
        "-t",
        s"1-${taskArraySize}",
        "-binding",
        s"linear:${numCores}",
        "-pe",
        "smp",
        numCores.toString) ++
      queuePart ++
      Seq(
        "-l",
        s"${runTimePart},${memPart}") ++
      osPart :+ 
      drmScriptFile.toAbsolutePath.toString
    }

    actualExecutable +: (staticPartFromUgerConfig.toList ++ dynamicPart)
  }
    
  final def commandInvoker(
      sessionSource: SessionSource,
      ugerConfig: UgerConfig,
      actualExecutable: String = "qsub",
      scheduler: Scheduler = IOScheduler())(implicit ec: ExecutionContext): CommandInvoker[Params] = {

    def invocationFn(params: Params): Try[RunResults] = {
      val tokens = makeTokens(sessionSource, actualExecutable, params)
      
      debug(s"Invoking '$actualExecutable': '${tokens.mkString(" ")}'")
      
      Processes.runSync(actualExecutable, tokens)
    }
    
    val justOnce = new CommandInvoker.JustOnce[Params](actualExecutable, invocationFn)
    
    new CommandInvoker.Retrying(justOnce, ugerConfig.maxRetries, scheduler = scheduler)
  }
}

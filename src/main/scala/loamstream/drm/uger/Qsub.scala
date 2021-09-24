package loamstream.drm.uger

import java.nio.file.Path

import scala.util.Try

import loamstream.conf.UgerConfig
import loamstream.drm.DrmTaskArray
import loamstream.model.execute.DrmSettings
import loamstream.util.CommandInvoker
import loamstream.util.LogContext
import loamstream.util.Processes
import loamstream.util.RunResults
import monix.execution.Scheduler
import scala.collection.compat._

/**
 * @author clint
 * Jul 24, 2020
 */
object Qsub {
  type InvocationFn[A] = A => Try[RunResults]
  
  final case class Params(
      ugerConfig: UgerConfig, 
      settings: DrmSettings, 
      taskArraySize: Int, 
      drmScriptFile: Path,
      stdOutPathTemplate: String,
      stdErrPathTemplate: String)
  
  object Params {
    def apply(ugerConfig: UgerConfig, settings: DrmSettings, taskArray: DrmTaskArray): Params = {
      new Params(
          ugerConfig, 
          settings, 
          taskArray.size, 
          taskArray.drmScriptFile, 
          taskArray.stdOutPathTemplate, 
          taskArray.stdErrPathTemplate)
    }
  }
  
  private[uger] def makeTokens(actualExecutable: String, params: Params): Seq[String] = {
    import params.{ drmScriptFile, settings, taskArraySize, ugerConfig } 
    
    val staticPartFromUgerConfig = {
      ugerConfig.staticJobSubmissionParams.split("\\s+").iterator.map(_.trim).filter(_.nonEmpty)
    }

    val dynamicPart = {
      import settings._

      val numCores = cores.value
      val runTimeInHours: Int = maxRunTime.hours.toInt
      val mem: Int = memoryPerCore.gb.toInt

      val queuePart = queue.map(q => Seq("-q", q.name)).getOrElse(Nil)

      val osPart = containerParams.map(_ => Seq("-l", "os=RedHat7")).getOrElse(Nil)
      
      val memPart = s"h_vmem=${mem}G"
      val runTimePart = s"h_rt=${runTimeInHours}:0:0"
      val runtimeAndMemPart = Seq("-l", s"${runTimePart},${memPart}")

      val taskArrayPart = Seq("-t", s"1-${taskArraySize}")
      
      val stdoutPathPart = Seq("-o", params.stdOutPathTemplate)
      val stderrPathPart = Seq("-e", params.stdErrPathTemplate)

      val numCoresPart = Seq(
        "-binding",
        s"linear:${numCores}",
        "-pe",
        "smp",
        numCores.toString)
      
      taskArrayPart ++
      numCoresPart ++
      queuePart ++
      runtimeAndMemPart ++
      stdoutPathPart ++ 
      stderrPathPart ++
      osPart :+ 
      drmScriptFile.toAbsolutePath.toString
    }

    actualExecutable +: (staticPartFromUgerConfig.to(List) ++ dynamicPart)
  }
    
  final def commandInvoker(
      ugerConfig: UgerConfig,
      actualExecutable: String = "qsub",
      scheduler: Scheduler)(implicit logCtx: LogContext): CommandInvoker.Async[Params] = {

    def invocationFn(params: Params): Try[RunResults] = Try {
      val tokens = makeTokens(actualExecutable, params)
      
      logCtx.debug(s"Invoking '$actualExecutable': '${tokens.mkString(" ")}'")
      
      Processes.runSync(tokens)()
    }
    
    val justOnce = new CommandInvoker.Async.JustOnce[Params](
      actualExecutable, 
      invocationFn,
      isSuccess = RunResults.SuccessPredicate.zeroIsSuccess)(scheduler, logCtx)
    
    new CommandInvoker.Async.Retrying(justOnce, ugerConfig.maxRetries, scheduler = scheduler)
  }
}

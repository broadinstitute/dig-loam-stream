package loamstream.drm.uger

import loamstream.util.CommandInvoker
import loamstream.util.RunResults
import loamstream.util.Loggable
import scala.util.Try
import loamstream.util.Processes
import scala.concurrent.ExecutionContext
import rx.lang.scala.schedulers.IOScheduler
import rx.lang.scala.Scheduler
import loamstream.util.Iterators
import loamstream.util.Options

/**
 * @author clint
 * Jul 24, 2020
 */
object Qconf extends Loggable {
  private[uger] def makeCreateTokens(actualExecutable: String = "qconf"): Seq[String] = {
    Seq(actualExecutable, "-csi")
  }
  
  private[uger] def makeDeleteTokens(actualExecutable: String = "qconf", sessionId: String): Seq[String] = {
    Seq(actualExecutable, "-dsi", sessionId)
  }
  
  private[uger] def createCommandInvoker(
      maxRetries: Int, 
      actualExecutable: String = "qconf",
      //TODO
      scheduler: Scheduler = IOScheduler())(implicit ec: ExecutionContext): CommandInvoker[Unit] = {
    
    def invocationFn(ignored: Unit): Try[RunResults] = {
      val tokens = makeCreateTokens(actualExecutable)
      
      debug(s"Invoking '$actualExecutable': '${tokens.mkString(" ")}'")
      
      Processes.runSync(actualExecutable, tokens)
    }
    
    val justOnce = new CommandInvoker.JustOnce[Unit](actualExecutable, invocationFn)
    
    new CommandInvoker.Retrying(justOnce, maxRetries, scheduler = scheduler)
  }
  
  private[uger] def deleteCommandInvoker(
      maxRetries: Int, 
      actualExecutable: String = "qconf",
      //TODO
      scheduler: Scheduler = IOScheduler())(implicit ec: ExecutionContext): CommandInvoker[String] = {
    
    def invocationFn(sessionId: String): Try[RunResults] = {
      val tokens = makeDeleteTokens(actualExecutable, sessionId)
      
      debug(s"Invoking '$actualExecutable': '${tokens.mkString(" ")}'")
      
      Processes.runSync(actualExecutable, tokens)
    }
    
    val justOnce = new CommandInvoker.JustOnce[String](actualExecutable, invocationFn)
    
    new CommandInvoker.Retrying(justOnce, maxRetries, scheduler = scheduler)
  }
  
  private[uger] def parseOutput(lines: Seq[String]): Try[String] = {
    import Iterators.Implicits._
    
    val headOpt = lines.iterator.map(_.trim).nextOption()
    
    Options.toTry(headOpt)(s"Couldn't parse qconf output $lines")
  }
}

package loamstream.drm.lsf

import loamstream.drm.lsf.InvokesBjobs.InvocationFn
import loamstream.drm.AccountingClient
import loamstream.drm.Queue
import loamstream.util.Loggable
import loamstream.util.Tries
import scala.util.Try
import scala.util.Failure
import scala.util.Success

/**
 * @author clint
 * May 21, 2018
 */
final class BjobsAccountingClient private[lsf] (invocationFn: InvocationFn[String]) extends 
    AccountingClient with Loggable {
  
  import BjobsAccountingClient.AccountingInfo
  
  override def getExecutionNode(lsfJobId: String): Option[String] = getAccountingInfo(lsfJobId).map(_.execHost)

  override def getQueue(lsfJobId: String): Option[Queue] = getAccountingInfo(lsfJobId).map(_.queue)
  
  private def getAccountingInfo(lsfJobId: String): Option[AccountingInfo] = {
    invocationFn(lsfJobId) match { 
      case Success(runResults) => {
        if(runResults.isFailure) { 
          import runResults.exitCode
          
          runResults.logStdOutAndStdErr(
            s"Failed to get LSF accounting info for job id $lsfJobId (exit code ${exitCode}), stdout and stderr follow:")
          
          None
        } else {
          BjobsAccountingClient.parseBjobsOutput(runResults.stdout)
        }
      }
      case Failure(e) => {
        warn(s"Error getting accounting info for LSF job id ${lsfJobId} : ${e.getMessage}", e)
      
        None
      }
    }
  }
}

object BjobsAccountingClient extends InvokesBjobs.Companion(new BjobsAccountingClient(_)) {
  private[lsf] override def makeTokens(actualExecutable: String, jobId: String): Seq[String] = {
    //NB: See https://www.ibm.com/support/knowledgecenter/en/SSETD4_9.1.2/lsf_command_ref/bjobs.1.html
    Seq(
        actualExecutable, 
        "-noheader", //Don't print a header row
        "-d",        //"Displays information about jobs that finished recently"
        "-r",        //Displays running jobs
        "-s",        //Display suspended jobs
        "-o",        //Specify output columns
        "exec_host:-100 queue:-100", 
        jobId)
  }
  
  private[lsf] final case class AccountingInfo(execHost: String, queue: Queue)
  
  private[lsf] def parseBjobsOutput(lines: Seq[String]): Option[AccountingInfo] = {
    lines.headOption.map(_.trim).filter(_.nonEmpty).flatMap { line =>
      val liftedLineParts = line.split("\\s+").lift
      
      for {
        execHost <- liftedLineParts(0)
        queueString <- liftedLineParts(1)
      } yield AccountingInfo(execHost.trim, Queue(queueString.trim))
    }
  }
}

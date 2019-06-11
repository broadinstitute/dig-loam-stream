package loamstream.drm.uger

import java.time.Instant
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import java.time.format.DateTimeFormatterBuilder

import scala.collection.Seq
import scala.concurrent.duration.DurationDouble
import scala.util.Try
import scala.util.matching.Regex

import loamstream.conf.UgerConfig
import loamstream.drm.AccountingClient
import loamstream.drm.Queue
import loamstream.model.execute.Resources.UgerResources
import loamstream.model.quantities.CpuTime
import loamstream.model.quantities.Memory
import loamstream.util.Loggable
import loamstream.util.Options
import loamstream.util.RetryingCommandInvoker
import loamstream.util.Tries
import loamstream.model.jobs.TerminationReason
import scala.util.Success

/**
 * @author clint
 * Mar 9, 2017
 *
 * An AccountingClient that retrieves metadata from whitespace-seperated key-value output, like what's
 * produced by `qacct`.  Parameterized on the actual method of retrieving this output given a
 * job id, to facilitate unit testing.
 */
final class QacctAccountingClient(
    qacctInvoker: RetryingCommandInvoker[String]) extends AccountingClient with Loggable {

  import QacctAccountingClient._

  private def getQacctOutputFor(jobId: String): Try[Seq[String]] = qacctInvoker(jobId).map(_.stdout)

  import Regexes.{ cpu, endTime, hostname, mem, qname, startTime }

  override def getResourceUsage(jobId: String): Try[UgerResources] = {
    for {
      output <- getQacctOutputFor(jobId)
      nodeOpt = findField(output, hostname).toOption
      queueOpt = findField(output, qname).map(Queue(_)).toOption
      memory <- findField(output, mem).flatMap(toMemory)
      cpuTime <- findField(output, cpu).flatMap(toCpuTime)
      start <- findField(output, startTime).flatMap(toInstant("start"))
      end <- findField(output, endTime).flatMap(toInstant("end"))
    } yield {
      UgerResources(
        memory = memory,
        cpuTime = cpuTime,
        node = nodeOpt,
        queue = queueOpt,
        startTime = start,
        endTime = end,
        raw = Some(output.mkString(System.lineSeparator)))
    }
  }
  
  override def getTerminationReason(jobId: String): Try[Option[TerminationReason]] = {
    //NB: Uger/qacct does not provide this information directly. 
    Success(None)
  }
}

object QacctAccountingClient extends Loggable {

  /**
   * Make a QacctAccountingClient that will retrieve job metadata by running some executable, by default, `qacct`.
   */
  def useActualBinary(ugerConfig: UgerConfig, binaryName: String = "qacct"): QacctAccountingClient = {
    new QacctAccountingClient(QacctInvoker.useActualBinary(ugerConfig.maxQacctRetries, binaryName))
  }
  
  private def orElseErrorMessage[A](msg: String)(a: => A): Try[A] = {
    Try(a).recoverWith { case _ => Tries.failure(msg) } 
  }
  
  //NB: Uger reports cpu time as a floating-point number of cpu-seconds. 
  private def toCpuTime(s: String) = {
    orElseErrorMessage(s"Couldn't parse '$s' as CpuTime") {
      CpuTime(s.toDouble.seconds)
    }
  }
  
  //NB: The value of qacct's ru_maxrss field (in kilobytes) is the closest approximation of
  //a Uger job's memory utilization
  private def toMemory(s: String): Try[Memory] = {
    orElseErrorMessage(s"Couldn't parse '$s' as Memory (in kilobytes)") {
      Memory.inKb(s.toDouble)
    }
  }
  
  //NB: qacct reports timestamps in a format like `03/06/2017 17:49:50.455` in the local time zone 
  private[uger] def toInstant(fieldType: String)(s: String): Try[Instant] = {
    orElseErrorMessage(s"Couldn't parse $fieldType timestamp from '$s'") {
      QacctAccountingClient.dateFormatter.parse(s, Instant.from(_))
    }
  }

  private def findField(fields: Seq[String], regex: Regex): Try[String] = {
    val opt = fields.collectFirst { case regex(value) => value.trim }.filter(_.nonEmpty)
    
    Options.toTry(opt)(s"Couldn't find field that matched regex '$regex'")
  }
  
  //Example date from qacct: 03/06/2017 17:49:50.455
  private val dateFormatter: DateTimeFormatter = {
    (new DateTimeFormatterBuilder)
      .appendPattern("MM/dd/yyyy HH:mm:ss.SSS")
      .toFormatter
      .withZone(ZoneId.systemDefault)
  }
  
  private object Regexes {
    val qname = "qname\\s+(.+?)$".r
    val hostname = "hostname\\s+(.+?)$".r
    
    val cpu = "cpu\\s+(.+?)$".r
    val mem = "ru_maxrss\\s+(.+?)$".r
    
    val startTime = "start_time\\s+(.+?)$".r
    
    val endTime = "end_time\\s+(.+?)$".r
  }
}

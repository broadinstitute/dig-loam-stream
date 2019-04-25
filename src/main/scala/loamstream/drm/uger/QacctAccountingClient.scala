package loamstream.drm.uger

import scala.util.control.NonFatal
import scala.util.matching.Regex
import loamstream.drm.AccountingClient
import loamstream.drm.Queue
import loamstream.util.Functions
import loamstream.util.Loggable
import scala.collection.Seq
import scala.sys.process.stringSeqToProcess
import loamstream.conf.UgerConfig
import scala.util.Try
import scala.util.Success
import scala.util.Failure
import scala.concurrent.duration._
import loamstream.util.RunResults
import loamstream.util.Processes
import loamstream.model.execute.Resources.UgerResources
import loamstream.model.quantities.CpuTime
import loamstream.model.quantities.Memory
import loamstream.util.Options
import java.time.Instant
import java.time.format.DateTimeFormatter
import java.time.ZoneId
import java.time.format.DateTimeFormatterBuilder
import java.time.temporal.ChronoField
import loamstream.util.Tries
import java.time.LocalDateTime
import java.time.ZonedDateTime
import java.time.OffsetDateTime

/**
 * @author clint
 * Mar 9, 2017
 *
 * An AccountingClient that retrieves metadata from whitespace-seperated key-value output, like what's
 * produced by `qacct`.  Parameterized on the actual method of retrieving this output given a
 * job id, to facilitate unit testing.
 */
final class QacctAccountingClient(
    ugerConfig: UgerConfig,
    binaryName: String,
    val qacctOutputForJobIdFn: AccountingClient.InvocationFn[String],
    delayStart: Duration = AccountingClient.defaultDelayStart,
    delayCap: Duration = AccountingClient.defaultDelayCap) extends AccountingClient with Loggable {

  import QacctAccountingClient._

  //Memoize the function that retrieves the metadata, to avoid running something expensive, like invoking
  //qacct in the production case, more than necessary.
  //NB: If qacct fails, retry up to ugerConfig.maxQacctRetries times, by default waiting 
  //0.5, 1, 2, 4, ... up to 30s in between each one.
  private val qacctOutputForJobId: AccountingClient.InvocationFn[String] = {
    AccountingClient.doRetries(
        binaryName = binaryName, 
        maxRetries = ugerConfig.maxQacctRetries, 
        delayStart = delayStart, 
        delayCap = delayCap, 
        delegateFn = qacctOutputForJobIdFn)
  }

  //NB: Failures will already have been logged, so we can drop any Failure(e) here.
  private def getQacctOutputFor(jobId: String): Try[Seq[String]] = {
    qacctOutputForJobId(jobId).map(_.stdout)
  }

  import Regexes.{ hostname, qname, cpu, mem, startTime, endTime }

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
        endTime = end)
    }
  }
}

object QacctAccountingClient extends Loggable {

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

  /**
   * Make a QacctAccountingClient that will retrieve job metadata by running some executable, by default, `qacct`.
   */
  def useActualBinary(ugerConfig: UgerConfig, binaryName: String = "qacct"): QacctAccountingClient = {
    def invokeQacctFor(jobId: String): Try[RunResults] = Processes.runSync(binaryName, makeTokens(binaryName, jobId))
    
    new QacctAccountingClient(ugerConfig, binaryName, invokeQacctFor)
  }

  private[uger] def makeTokens(binaryName: String, jobId: String): Seq[String] = Seq(binaryName, "-j", jobId)
}

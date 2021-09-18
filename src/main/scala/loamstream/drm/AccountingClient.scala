package loamstream.drm

import loamstream.model.execute.Resources.DrmResources
import loamstream.model.jobs.TerminationReason
import monix.eval.Task
import loamstream.util.Tries
import loamstream.model.jobs.DrmJobOracle
import scala.util.Try
import loamstream.util.Files
import java.nio.file.Path
import loamstream.util.CanBeClosed
import loamstream.util.Options
import loamstream.model.quantities.Memory
import java.time.LocalDateTime
import loamstream.model.quantities.CpuTime


/**
 * @author clint
 * Mar 9, 2017
 *
 * An abstraction for getting some environment-specific metadata that can't currently be accessed via DRMAA
 */
trait AccountingClient {
  def getResourceUsage(taskId: DrmTaskId): Task[DrmResources]
  
  def getTerminationReason(taskId: DrmTaskId): Task[Option[TerminationReason]]
  
  def getAccountingInfo(taskId: DrmTaskId): Task[AccountingInfo] = {
    val rsf = getResourceUsage(taskId)
    val trf = getTerminationReason(taskId)
    
    Task.parMap2(rsf, trf)(AccountingInfo(_, _))
  }
}

object AccountingClient {
  final class AlwaysFailsAccountingClient(drmSystem: DrmSystem) extends AccountingClient {
    override def getResourceUsage(taskId: DrmTaskId): Task[DrmResources] = {
      //Task.fromTry(Tries.failure("getResourceUsage() is disabled"))

      Task.now {
        drmSystem.resourcesMaker(
          Memory.inKb(123),
          CpuTime.inSeconds(1.23),
          Option("unknown-node"),
          None,
          LocalDateTime.now,
          LocalDateTime.now,
          None)
      }
    }

    override def getTerminationReason(taskId: DrmTaskId): Task[Option[TerminationReason]] = Task(None)
  }

  object StatsFileAccountingClient {
    private val statsLineRegex = "^(.+?)\\:(.*)$".r
  }

  final class StatsFileAccountingClient(drmSystem: DrmSystem, oracle: DrmJobOracle) extends AccountingClient {
    def getResourceUsage(taskId: DrmTaskId): Task[DrmResources] = {
      def statsFileLines(file: Path): Seq[String] = CanBeClosed.using(scala.io.Source.fromFile(file.toFile)) { 
        _.getLines.map(_.trim).filter(_.nonEmpty).toList
      }

      def parseStatsFile(file: Path): Try[Map[String, String]] = {
        val tuples: Try[Seq[(String, String)]] = Try {
          statsFileLines(file).map {
            case StatsFileAccountingClient.statsLineRegex(key, value) => (key, value)
          }
        }

        tuples.map(_.toMap)
      }

      def doLookup(key: String)(data: Map[String, String], statsFile: Path): Try[String] = {
        Options.toTry(data.get(key))(s"Couldn't find key '$key' in stats file at '$statsFile', contents: '$data'")
      }

      def dropTrailing(ch: Char)(s: String): String = if(s.last == ch) s.dropRight(1) else s

      val attempt: Try[DrmResources] = for {
        jobDir <- Try(oracle.dirFor(taskId))
        statsFile <- Files.tryFile(jobDir.resolve("stats"))
        statsData <- parseStatsFile(statsFile)
        lookup = (key: String) => doLookup(key)(statsData, statsFile)
        memoryInK <- lookup("Memory").map(dropTrailing('k')).map(_.toDouble).map(Memory.inKb)
        systime <- lookup("System").map(dropTrailing('s')).map(_.toLong)
        usertime <- lookup("User").map(dropTrailing('s')).map(_.toLong)
        node <- lookup("Node")
        start <- lookup("Start").map(LocalDateTime.parse)
        end <- lookup("End").map(LocalDateTime.parse)
      } yield {
        val cpuTime = CpuTime.inSeconds(systime + usertime)
        val queue = None

        drmSystem.resourcesMaker(
          memoryInK,
          cpuTime,
          Option(node),
          queue,
          start,
          end,
          Option(statsFileLines(statsFile).mkString(System.lineSeparator))
        )
      }

      Task.fromTry(attempt)
    }

    override def getTerminationReason(taskId: DrmTaskId): Task[Option[TerminationReason]] = Task.now(None)
  }
}
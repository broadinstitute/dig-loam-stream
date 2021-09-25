package loamstream.drm

import loamstream.model.quantities.Memory
import loamstream.model.quantities.CpuTime
import loamstream.model.jobs.TerminationReason
import loamstream.model.execute.Resources
import java.time.LocalDateTime
import java.nio.file.Path
import scala.util.Try
import scala.util.matching.Regex
import loamstream.util.Files
import loamstream.util.CanBeClosed
import scala.io.Source
import loamstream.util.Options
import org.scalafmt.config.Docstrings


/**
  * @author clint
  * @date Jun 23, 2021
  *
  */
final case class ExecutionStats(
  exitCode: Int,
  memory: Option[Memory],
  cpu: CpuTime,
  startTime: LocalDateTime,
  endTime: LocalDateTime,
  terminationReason: Option[TerminationReason]
) {

  def toDrmResources(drmSystem: DrmSystem)(
      node: Option[String] = None,
      queue: Option[Queue] = None,
      derivedFrom: Option[String]): Option[Resources.DrmResources] = {

    memory.map(drmSystem.resourcesMaker(_, cpu, node, queue, startTime, endTime, derivedFrom))
  }
}

object ExecutionStats {
  private object Regexes {
    //val exitCode = """^ExitCode\:\s*(\d+).*$""".r
    val memory = """^(\d+)k$""".r
    val systime = """^(.+)s$""".r
    val usertime = """^(.+)s$""".r
    //val startTime = """^Start\:\s*(.+)$""".r
    //val endTime = """^End\:\s*(.+)$""".r

    val keyValueLine = """^(.+?)\s+(.+)$""".r
  }

  private object Fields {
    val exitCode = "exitcode"
    val memory = "memory"
    val systime = "system"
    val usertime = "user"
    val startTime = "start"
    val endTime = "end"
  }

  def fromFile(file: Path): Try[ExecutionStats] = {
    
    def byFieldAttempt: Try[Map[String, String]] = Try {
      def normalize(s: String): String = s.trim.toLowerCase

      val lines = CanBeClosed.using(Source.fromFile(file.toFile))(_.getLines.map(_.trim).filter(_.nonEmpty).toList)

      lines.iterator.collect { case Regexes.keyValueLine(k, v) => normalize(k) -> normalize(v) }.toMap
    }

    def toLocalDateTime(s: String): Try[LocalDateTime] = Try(LocalDateTime.parse(s))

    def trimTrailing(ch: Char)(s: String): String = if(s.last == ch) s.dropRight(1) else s

    def parseMem(s: String) = Try(trimTrailing('k')(s).toDouble).map(Memory.inKb)
    def parseTime(s: String) = Try(trimTrailing('s')(s).toDouble)

    for {
      byField <- byFieldAttempt
      getField = (field: String) => Options.toTry(byField.get(field))(s"Couldn't find '$field' in $file")
      memory = getField(Fields.memory).flatMap(parseMem).toOption
      exitCode <- getField(Fields.exitCode).map(_.toInt)
      systime <- getField(Fields.systime).flatMap(parseTime)
      usertime <- getField(Fields.usertime).flatMap(parseTime)
      startTime <- getField(Fields.startTime).flatMap(toLocalDateTime)
      endTime <- getField(Fields.endTime).flatMap(toLocalDateTime)
    } yield {
      val cpuTime = CpuTime.inSeconds(systime + usertime)

      ExecutionStats(
        exitCode = exitCode,
        memory = memory,
        cpu = cpuTime,
        startTime = startTime,
        endTime = endTime,
        terminationReason = None //TODO
      )
    }
  }
}
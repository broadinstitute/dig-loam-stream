package loamstream.conf

import java.nio.file.Path

/**
 * @author clint
 * Dec 17, 2018
 *
 * An object containing the location of various things: output files, default log dir, etc.
 */
trait Locations {
  def loamstreamDir: Path

  def jobDir: Path
  
  def jobDataDir: Path
  
  def logDir: Path

  def dryRunOutputFile: Path

  def dbDir: Path

  def ugerDir: Path
  def lsfDir: Path
}

object Locations {
  
  import java.nio.file.Files.createDirectories
  import java.nio.file.Paths.{ get => path }
  import loamstream.util.Paths.Implicits._
  
  final case class Literal(
    loamstreamDir: Path = Default.loamstreamDir,
    jobDir: Path = Default.jobDir,
    logDir: Path = Default.logDir,
    dryRunOutputFile: Path = Default.dryRunOutputFile,
    dbDir: Path = Default.dbDir,
    ugerDir: Path = Default.ugerDir,
    lsfDir: Path = Default.lsfDir) extends Locations {
    
    override lazy val jobDataDir: Path = createDirectories(jobDir / "data")
  }
  
  object Default extends Locations {
    override val loamstreamDir: Path = createDirectories(path("./.loamstream"))

    override val jobDir: Path = createDirectories((loamstreamDir / "jobs").normalize)
    
    override val jobDataDir: Path = createDirectories(jobDir / "data")

    override val logDir: Path = createDirectories((loamstreamDir / "logs").normalize)

    override val dryRunOutputFile: Path = (logDir / "joblist").normalize

    override val dbDir: Path = createDirectories(loamstreamDir / "db")

    override lazy val ugerDir = createDirectories((loamstreamDir / "uger").normalize)
    override lazy val lsfDir = createDirectories((loamstreamDir / "lsf").normalize)
  }
}

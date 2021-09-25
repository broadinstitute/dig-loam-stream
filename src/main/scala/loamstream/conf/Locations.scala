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

  def jobDataDir: Path
  
  def workerDir: Path
  
  def logDir: Path

  def dryRunOutputFile: Path

  def dbDir: Path

  def ugerDir: Path
  def lsfDir: Path
  def slurmDir: Path
}

object Locations {
  
  import java.nio.file.Paths.{ get => path }
  import loamstream.util.Paths.Implicits._
  
  lazy val Default = DefaultsIn(path(".loamstream").normalize)
  
  final case class DefaultsIn(loamstreamDir: Path) extends Locations {
    override val workerDir: Path = (loamstreamDir / "workers").normalize
    
    private[this] val jobDir: Path = (loamstreamDir / "jobs").normalize
    
    override val jobDataDir: Path = jobDir / "data"
    
    override val logDir: Path = (loamstreamDir / "logs").normalize

    override val dryRunOutputFile: Path = (logDir / "joblist").normalize

    override val dbDir: Path = loamstreamDir / "db"

    override lazy val ugerDir = (loamstreamDir / "uger").normalize
    override lazy val lsfDir = (loamstreamDir / "lsf").normalize
    override lazy val slurmDir = (loamstreamDir / "slurm").normalize
  }
}

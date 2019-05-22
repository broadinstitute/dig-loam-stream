package loamstream.conf

import com.typesafe.config.Config
import scala.util.Try
import java.nio.file.Path
import java.nio.file.Paths
import ExecutionConfig.Defaults
import scala.concurrent.duration.Duration

/**
 * @author clint
 * Apr 20, 2017
 */
final case class ExecutionConfig(
    maxRunsPerJob: Int = Defaults.maxRunsPerJob, 
    maxWaitTimeForOutputs: Duration = Defaults.maxWaitTimeForOutputs,
    outputPollingFrequencyInHz: Double = Defaults.outputPollingFrequencyInHz,
    dryRunOutputFile: Path = Defaults.dryRunOutputFile,
    anonStoreDir: Path = Defaults.anonStoreDir,
    singularity: SingularityConfig = Defaults.singularityConfig,
    dbDir: Path = Defaults.dbDir,
    logDir: Path = Defaults.logDir,
    jobDir: Path = Defaults.jobDir) {
  
  def toLocations: Locations = Locations.Literal(dbDir = dbDir, logDir = logDir, jobDir = jobDir)
}

object ExecutionConfig extends ConfigParser[ExecutionConfig] {

  object Defaults {
    val maxRunsPerJob: Int = 4 //scalastyle:ignore magic.number
  
    val dryRunOutputFile: Path = Locations.Default.dryRunOutputFile
    
    import scala.concurrent.duration._
    
    val maxWaitTimeForOutputs: Duration = 30.seconds
    
    val outputPollingFrequencyInHz: Double = 0.1
    
    val anonStoreDir: Path = Paths.get(System.getProperty("java.io.tmpdir", "/tmp"))
    
    val singularityConfig: SingularityConfig = SingularityConfig.default
    
    val dbDir: Path = Locations.Default.dbDir
    
    val logDir: Path = Locations.Default.logDir
    
    val jobDir: Path = Locations.Default.jobDir
  }
  
  val default: ExecutionConfig = ExecutionConfig()
  
  //Parse Typesafe Configs into instances of Parsed, which only contains those fields we want to be configurable
  //via loamstream.conf.  Other values (workDir, scriptDir) can be set by unit tests, for example, but adding them
  //to loamstream.conf has no effect.
  private final case class Parsed(
    maxRunsPerJob: Int = Defaults.maxRunsPerJob, 
    maxWaitTimeForOutputs: Duration = Defaults.maxWaitTimeForOutputs,
    outputPollingFrequencyInHz: Double = Defaults.outputPollingFrequencyInHz,
    anonStoreDir: Path = Defaults.anonStoreDir,
    singularity: SingularityConfig = Defaults.singularityConfig) {
    
    def toExecutionConfig: ExecutionConfig = ExecutionConfig(
      maxRunsPerJob = maxRunsPerJob, 
      maxWaitTimeForOutputs = maxWaitTimeForOutputs,
      outputPollingFrequencyInHz = outputPollingFrequencyInHz,
      anonStoreDir = anonStoreDir,
      singularity = singularity)
  }
  
  override def fromConfig(config: Config): Try[ExecutionConfig] = {
    import net.ceedubs.ficus.Ficus._
    import net.ceedubs.ficus.readers.ArbitraryTypeReader._
    import ValueReaders.PathReader
    import ValueReaders.SingularityConfigReader

    //NB: Ficus now marshals the contents of loamstream.execution into an ExecutionConfig instance.
    //Names of fields in ExecutionConfig and keys under loamstream.execution must match.
    Try(config.as[Parsed]("loamstream.execution").toExecutionConfig)
  }
}

package loamstream.conf

import java.nio.file.Path

import loamstream.model.quantities.CpuTime
import loamstream.model.quantities.Cpus
import loamstream.model.quantities.Memory
import loamstream.uger.UgerDefaults
import loamstream.util.Loggable
import scala.util.Try
import com.typesafe.config.Config
import loamstream.util.Tries

/**
 * @author clint
 * May 10, 2018
 */
sealed trait DrmConfig {
  def workDir: Path 
  
  def maxNumJobs: Int
  
  def defaultCores: Cpus
  
  def defaultMemoryPerCore: Memory
  
  def defaultMaxRunTime: CpuTime
}

object DrmConfig extends ConfigParser[DrmConfig] with Loggable {
  override def fromConfig(config: Config): Try[DrmConfig] = {
    if(config.hasPath("loamstream.uger") && config.hasPath("loamstream.lsf")) {
      Tries.failure(s"Either 'loamstream.uger' OR 'loamstream.lsf' can be defined, but both are present.")
    } else {
      UgerConfig.fromConfig(config).orElse(LsfConfig.fromConfig(config))
    }
  }
}

/**
  * Created on: 5/4/16
  *
  * @author Kaan Yuksel
  */
final case class UgerConfig(
    workDir: Path, 
    maxNumJobs: Int = UgerDefaults.maxConcurrentJobs,
    defaultCores: Cpus = UgerDefaults.cores,
    defaultMemoryPerCore: Memory = UgerDefaults.memoryPerCore,
    defaultMaxRunTime: CpuTime = UgerDefaults.maxRunTime) extends DrmConfig

object UgerConfig extends ConfigParser[UgerConfig] with Loggable {

  override def fromConfig(config: Config): Try[UgerConfig] = {
    import net.ceedubs.ficus.Ficus._
    import net.ceedubs.ficus.readers.ArbitraryTypeReader._
    import ValueReaders.PathReader
    import ValueReaders.MemoryReader
    import ValueReaders.CpuTimeReader
    import ValueReaders.CpusReader

    trace("Parsing Uger config...")
    
    //NB: Ficus marshals the contents of loamstream.uger into a UgerConfig instance.
    //Names of fields in UgerConfig and keys under loamstream.uger must match.
    
    Try(config.as[UgerConfig]("loamstream.uger"))
  }
}

/**
  * May 10, 2018
  *
  * @author Kaan Yuksel
  * @author clint
  */
final case class LsfConfig(
    workDir: Path, 
    maxNumJobs: Int = UgerDefaults.maxConcurrentJobs,
    defaultCores: Cpus = UgerDefaults.cores,
    defaultMemoryPerCore: Memory = UgerDefaults.memoryPerCore,
    defaultMaxRunTime: CpuTime = UgerDefaults.maxRunTime) extends DrmConfig

object LsfConfig extends ConfigParser[LsfConfig] with Loggable {

  override def fromConfig(config: Config): Try[LsfConfig] = {
    import net.ceedubs.ficus.Ficus._
    import net.ceedubs.ficus.readers.ArbitraryTypeReader._
    import ValueReaders.PathReader
    import ValueReaders.MemoryReader
    import ValueReaders.CpuTimeReader
    import ValueReaders.CpusReader

    trace("Parsing LSF config...")
    
    //NB: Ficus marshals the contents of loamstream.lsf into a LsfConfig instance.
    //Names of fields in LsfConfig and keys under loamstream.lsf must match.
    
    Try(config.as[LsfConfig]("loamstream.lsf"))
  }
}

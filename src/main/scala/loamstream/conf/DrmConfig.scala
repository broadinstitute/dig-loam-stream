package loamstream.conf

import java.nio.file.Path

import scala.util.Try

import com.typesafe.config.Config

import loamstream.drm.ScriptBuilderParams
import loamstream.drm.lsf.LsfDefaults
import loamstream.drm.lsf.LsfScriptBuilderParams
import loamstream.drm.slurm.SlurmDefaults
import loamstream.drm.slurm.SlurmScriptBuilderParams
import loamstream.drm.uger.UgerDefaults
import loamstream.drm.uger.UgerScriptBuilderParams
import loamstream.model.quantities.CpuTime
import loamstream.model.quantities.Cpus
import loamstream.model.quantities.Memory
import loamstream.util.Loggable

/**
 * @author clint
 * May 10, 2018
 */
sealed trait DrmConfig {
  def workDir: Path 
  
  def maxNumJobsPerTaskArray: Int
  
  def defaultCores: Cpus
  
  def defaultMemoryPerCore: Memory
  
  def defaultMaxRunTime: CpuTime
  
  final def isUgerConfig: Boolean = this.isInstanceOf[UgerConfig]
  
  final def isLsfConfig: Boolean = this.isInstanceOf[LsfConfig]
  
  def scriptBuilderParams: ScriptBuilderParams
  
  def maxRetries: Int
}

/**
  * Created on: 5/4/16
  *
  * @author Kaan Yuksel
  */
final case class UgerConfig(
    workDir: Path = Locations.Default.ugerDir,
    maxNumJobsPerTaskArray: Int = UgerDefaults.maxNumJobsPerTaskArray,
    defaultCores: Cpus = UgerDefaults.cores,
    defaultMemoryPerCore: Memory = UgerDefaults.memoryPerCore,
    defaultMaxRunTime: CpuTime = UgerDefaults.maxRunTime,
    extraPathDir: Path = UgerDefaults.extraPathDir,
    condaEnvName: String = UgerDefaults.condaEnvName,
    staticJobSubmissionParams: String = UgerDefaults.staticJobSubmissionParams,
    maxRetries: Int = UgerDefaults.maxRetries,
    maxQacctCacheSize: Int = UgerDefaults.maxQacctCacheSize) extends DrmConfig {
  
  override def scriptBuilderParams: ScriptBuilderParams = new UgerScriptBuilderParams(extraPathDir, condaEnvName)
}

object UgerConfig extends ConfigParser[UgerConfig] with Loggable {

  private final case class Parsed(
    maxNumJobsPerTaskArray: Int = UgerDefaults.maxNumJobsPerTaskArray,
    defaultCores: Cpus = UgerDefaults.cores,
    defaultMemoryPerCore: Memory = UgerDefaults.memoryPerCore,
    defaultMaxRunTime: CpuTime = UgerDefaults.maxRunTime,
    extraPathDir: Path = UgerDefaults.extraPathDir,
    condaEnvName: String = UgerDefaults.condaEnvName,
    staticJobSubmissionParams: String = UgerDefaults.staticJobSubmissionParams,
    maxRetries: Int = UgerDefaults.maxRetries,
    maxQacctCacheSize: Int = UgerDefaults.maxQacctCacheSize) {
    
    def toUgerConfig: UgerConfig = UgerConfig(
      maxNumJobsPerTaskArray = maxNumJobsPerTaskArray,
      defaultCores = defaultCores,
      defaultMemoryPerCore = defaultMemoryPerCore,
      defaultMaxRunTime = defaultMaxRunTime,
      extraPathDir = extraPathDir,
      condaEnvName = condaEnvName,
      staticJobSubmissionParams = staticJobSubmissionParams,
      maxRetries = maxRetries,
      maxQacctCacheSize = maxQacctCacheSize)
  }
  
  override def fromConfig(config: Config): Try[UgerConfig] = {
    import net.ceedubs.ficus.Ficus._
    import net.ceedubs.ficus.readers.ArbitraryTypeReader._
  import net.ceedubs.ficus.Ficus._
    import net.ceedubs.ficus.readers.ArbitraryTypeReader._
    import ValueReaders.PathReader
    import ValueReaders.MemoryReader
    import ValueReaders.CpusReader
    import ValueReaders.CpuTimeReader
    
    trace("Parsing Uger config...")
    
    //NB: Ficus marshals the contents of loamstream.uger into a UgerConfig instance.
    //Names of fields in UgerConfig and keys under loamstream.uger must match.
    
    Try(config.as[Parsed]("loamstream.uger").toUgerConfig)
  }
}

/**
  * May 10, 2018
  *
  * @author Kaan Yuksel
  * @author clint
  */
final case class LsfConfig(
    workDir: Path = Locations.Default.lsfDir,
    maxNumJobsPerTaskArray: Int = LsfDefaults.maxNumJobsPerTaskArray,
    defaultCores: Cpus = LsfDefaults.cores,
    defaultMemoryPerCore: Memory = LsfDefaults.memoryPerCore,
    defaultMaxRunTime: CpuTime = LsfDefaults.maxRunTime,
    maxBacctRetries: Int =  LsfDefaults.maxBacctRetries,
    maxRetries: Int = LsfDefaults.maxRetries) extends DrmConfig {
  
  override def scriptBuilderParams: ScriptBuilderParams = LsfScriptBuilderParams
}

object LsfConfig extends ConfigParser[LsfConfig] with Loggable {
  //Parse Typesafe Configs into instances of Parsed, which only contains those fields we want to be configurable
  //via loamstream.conf.  Other values (workDir, scriptDir) can be set by unit tests, for example, but adding them
  //to loamstream.conf has no effect.
  private final case class Parsed(
    maxNumJobsPerTaskArray: Int = LsfDefaults.maxNumJobsPerTaskArray,
    defaultCores: Cpus = LsfDefaults.cores,
    defaultMemoryPerCore: Memory = LsfDefaults.memoryPerCore,
    defaultMaxRunTime: CpuTime = LsfDefaults.maxRunTime,
    maxBacctRetries: Int = LsfDefaults.maxBacctRetries,
    maxRetries: Int = LsfDefaults.maxRetries) {
    
    def toLsfConfig: LsfConfig = LsfConfig(
      maxNumJobsPerTaskArray = maxNumJobsPerTaskArray,
      defaultCores = defaultCores,
      defaultMemoryPerCore = defaultMemoryPerCore,
      defaultMaxRunTime = defaultMaxRunTime,
      maxBacctRetries = maxBacctRetries,
      maxRetries = maxRetries)
  }
  
  override def fromConfig(config: Config): Try[LsfConfig] = {
    import net.ceedubs.ficus.Ficus._
    import net.ceedubs.ficus.readers.ArbitraryTypeReader._
    import net.ceedubs.ficus.Ficus._
    import net.ceedubs.ficus.readers.ArbitraryTypeReader._
    import ValueReaders.PathReader
    import ValueReaders.MemoryReader
    import ValueReaders.CpusReader
    import ValueReaders.CpuTimeReader
    
    trace("Parsing LSF config...")
    
    //NB: Ficus marshals the contents of loamstream.lsf into a LsfConfig instance.
    //Names of fields in LsfConfig and keys under loamstream.lsf must match.
    
    Try(config.as[Parsed]("loamstream.lsf").toLsfConfig)
  }
}

/**
  * May 27, 2021
  *
  * @author clint
  */
final case class SlurmConfig(
    workDir: Path = Locations.Default.slurmDir,
    maxNumJobsPerTaskArray: Int = SlurmDefaults.maxNumJobsPerTaskArray,
    defaultCores: Cpus = SlurmDefaults.cores,
    defaultMemoryPerCore: Memory = SlurmDefaults.memoryPerCore,
    defaultMaxRunTime: CpuTime = SlurmDefaults.maxRunTime,
    maxSacctRetries: Int =  SlurmDefaults.maxSacctRetries,
    maxRetries: Int = SlurmDefaults.maxRetries) extends DrmConfig {
  
  override def scriptBuilderParams: ScriptBuilderParams = SlurmScriptBuilderParams
}

object SlurmConfig extends ConfigParser[SlurmConfig] with Loggable {
  //Parse Typesafe Configs into instances of Parsed, which only contains those fields we want to be configurable
  //via loamstream.conf.  Other values (workDir, scriptDir) can be set by unit tests, for example, but adding them
  //to loamstream.conf has no effect.
  private final case class Parsed(
    maxNumJobsPerTaskArray: Int = SlurmDefaults.maxNumJobsPerTaskArray,
    defaultCores: Cpus = SlurmDefaults.cores,
    defaultMemoryPerCore: Memory = SlurmDefaults.memoryPerCore,
    defaultMaxRunTime: CpuTime = SlurmDefaults.maxRunTime,
    maxSacctRetries: Int = SlurmDefaults.maxSacctRetries,
    maxRetries: Int = SlurmDefaults.maxRetries) {
    
    def toSlurmConfig: SlurmConfig = SlurmConfig(
      maxNumJobsPerTaskArray = maxNumJobsPerTaskArray,
      defaultCores = defaultCores,
      defaultMemoryPerCore = defaultMemoryPerCore,
      defaultMaxRunTime = defaultMaxRunTime,
      maxSacctRetries = maxSacctRetries,
      maxRetries = maxRetries)
  }
  
  override def fromConfig(config: Config): Try[SlurmConfig] = {
    import net.ceedubs.ficus.Ficus._
    import net.ceedubs.ficus.readers.ArbitraryTypeReader._
    import ValueReaders.PathReader
    import ValueReaders.MemoryReader
    import ValueReaders.CpusReader
    import ValueReaders.CpuTimeReader

    trace("Parsing LSF config...")
    
    //NB: Ficus marshals the contents of loamstream.slurm into a SlurmDefaults instance.
    //Names of fields in SlurmDefaults and keys under loamstream.slurm must match.
    
    Try(config.as[Parsed]("loamstream.slurm").toSlurmConfig)
  }
}

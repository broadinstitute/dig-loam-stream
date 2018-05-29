package loamstream.conf

import java.nio.file.Path

import scala.util.Try

import com.typesafe.config.Config

import loamstream.drm.ScriptBuilderParams
import loamstream.drm.lsf.LsfDefaults
import loamstream.drm.lsf.LsfScriptBuilderParams
import loamstream.drm.uger.UgerDefaults
import loamstream.drm.uger.UgerScriptBuilderParams
import loamstream.model.quantities.CpuTime
import loamstream.model.quantities.Cpus
import loamstream.model.quantities.Memory
import loamstream.util.Loggable
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
  
  final def isUgerConfig: Boolean = this.isInstanceOf[UgerConfig]
  
  final def isLsfConfig: Boolean = this.isInstanceOf[LsfConfig]
  
  //TODO: This feels wrong
  final def scriptBuilderParams: ScriptBuilderParams = {
    require(isUgerConfig || isLsfConfig)
    
    if(isUgerConfig) { UgerScriptBuilderParams }
    else { LsfScriptBuilderParams }
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
    import ValueReaders.CpusReader
    import ValueReaders.CpuTimeReader

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
    maxNumJobs: Int = LsfDefaults.maxConcurrentJobs,
    defaultCores: Cpus = LsfDefaults.cores,
    defaultMemoryPerCore: Memory = LsfDefaults.memoryPerCore,
    defaultMaxRunTime: CpuTime = LsfDefaults.maxRunTime) extends DrmConfig

object LsfConfig extends ConfigParser[LsfConfig] with Loggable {

  override def fromConfig(config: Config): Try[LsfConfig] = {
    import net.ceedubs.ficus.Ficus._
    import net.ceedubs.ficus.readers.ArbitraryTypeReader._
    import ValueReaders.PathReader
    import ValueReaders.MemoryReader
    import ValueReaders.CpusReader
    import ValueReaders.CpuTimeReader

    trace("Parsing LSF config...")
    
    //NB: Ficus marshals the contents of loamstream.lsf into a LsfConfig instance.
    //Names of fields in LsfConfig and keys under loamstream.lsf must match.
    
    Try(config.as[LsfConfig]("loamstream.lsf"))
  }
}
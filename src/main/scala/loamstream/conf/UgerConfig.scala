package loamstream.conf

import java.nio.file.Path

import com.typesafe.config.Config

import scala.util.Try
import loamstream.model.quantities.Memory
import loamstream.model.quantities.Cpus
import scala.concurrent.duration.Duration
import loamstream.uger.UgerDefaults
import loamstream.model.quantities.CpuTime
import loamstream.util.Loggable

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
    defaultMaxRunTime: CpuTime = UgerDefaults.maxRunTime)

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

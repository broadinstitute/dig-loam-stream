package loamstream.conf

import java.net.URI
import java.nio.file.Path
import java.nio.file.Paths

import scala.concurrent.duration.Duration

import com.typesafe.config.Config

import loamstream.model.quantities.Memory
import net.ceedubs.ficus.readers.ValueReader
import loamstream.model.quantities.CpuTime
import loamstream.model.quantities.Cpus

/**
 * @author clint
 * Feb 6, 2017
 * 
 * Typeclasses telling the Ficus config-parsing lib how to read various types from  
 * Typesafe config objects.
 */
object ValueReaders {
  implicit val PathReader: ValueReader[Path] = new ValueReader[Path] {
    override def read(config: Config, path: String): Path = Paths.get(config.getString(path))
  }
  
  implicit val MemoryReader: ValueReader[Memory] = new ValueReader[Memory] {
    override def read(config: Config, path: String): Memory = Memory.inGb(config.getDouble(path))
  }
  
  implicit val CpusReader: ValueReader[Cpus] = new ValueReader[Cpus] {
    override def read(config: Config, path: String): Cpus = Cpus(config.getInt(path))
  }
  
  implicit val CpuTimeReader: ValueReader[CpuTime] = new ValueReader[CpuTime] {
    import scala.concurrent.duration.DurationInt
    
    override def read(config: Config, path: String): CpuTime = CpuTime.inHours(config.getString(path).toDouble)
  }
  
  implicit val GcsUriReader: ValueReader[URI] = new ValueReader[URI] {
    override def read(config: Config, path: String): URI = {
      val result = new URI(config.getString(path))
      
      require(result.getScheme == "gs")
      
      result
    }
  }
}

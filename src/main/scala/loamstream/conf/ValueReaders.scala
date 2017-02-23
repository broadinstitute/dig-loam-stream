package loamstream.conf

import net.ceedubs.ficus.readers.ValueReader
import java.nio.file.Path
import com.typesafe.config.Config
import java.nio.file.Paths
import java.nio.file.Paths
import java.net.URI

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
  
  implicit val UriReader: ValueReader[URI] = new ValueReader[URI] {
    override def read(config: Config, path: String): URI = new URI(config.getString(path))
  }
}

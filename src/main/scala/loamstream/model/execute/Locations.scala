package loamstream.model.execute

import java.nio.file.Path
import java.net.URI

/**
 * @author clint
 * Jun 7, 2018
 */
trait Locations {
  def inHost(p: Path): Path
  
  def inContainer(p: Path): Path
  
  final def inHost(u: URI): URI = u
  
  final def inContainer(u: URI): URI = u
}

object Locations {
  object Identity extends Locations {
    override def inHost(p: Path): Path = p 
  
    override def inContainer(p: Path): Path = p
  }
}

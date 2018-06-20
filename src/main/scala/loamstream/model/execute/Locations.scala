package loamstream.model.execute

import java.nio.file.Path
import java.net.URI

/**
 * @author clint
 * Jun 7, 2018
 */
trait Locations[L] {
  def inHost(p: L): L
  
  def inContainer(p: L): L
}

object Locations {
  private object Identity extends Locations[Any] {
    override def inHost(a: Any): Any = a 
  
    override def inContainer(a: Any): Any = a
  }
  
  def identity[A]: Locations[A] = Identity.asInstanceOf[Locations[A]]
}

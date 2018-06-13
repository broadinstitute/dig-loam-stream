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
  def identity[A]: Locations[A] = new Locations[A] {
    override def inHost(a: A): A = a 
  
    override def inContainer(a: A): A = a
  }
}

package loamstream.util

import java.io.Closeable

/**
 * @author clint
 * Nov 23, 2016
 */
trait Terminable {
  def stop(): Unit
  
  def asCloseable: Closeable = () => stop()
}

object Terminable {
  trait StopsComponents extends Terminable { self: Loggable =>
    protected def terminableComponents: Iterable[Terminable]
    
    override def stop(): Unit = {
      def doStop(t: Terminable) = Throwables.quietly(s"Error stopping $t")(t.stop())
      
      terminableComponents.foreach(doStop)
    }
  }
  
  object StopsComponents {
    def apply(t: Terminable, rest: Terminable*): StopsComponents = new StopsComponents with Loggable {
      final override protected def terminableComponents: Iterable[Terminable] = t +: rest
    }
    
    def apply(c: Closeable, rest: Closeable*): StopsComponents = new StopsComponents with Loggable {
      final override protected def terminableComponents: Iterable[Terminable] = (c +: rest).map(from)
    }
  }
  
  def from(c: Closeable): Terminable = () => c.close()
  
  def apply(doStop: => Unit): Terminable = new Terminable {
    override def stop(): Unit = doStop
  }
  
  implicit final class CloseableOps(val c: Closeable) extends AnyVal {
    def asTerminable: Terminable = from(c)
  }
}

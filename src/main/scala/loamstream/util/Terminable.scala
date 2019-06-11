package loamstream.util

/**
 * @author clint
 * Nov 23, 2016
 */
trait Terminable {
  def stop(): Unit
}

object Terminable {
  trait StopsComponents extends Terminable { self: Loggable =>
    protected def terminableComponents: Iterable[Terminable]
    
    override def stop(): Unit = {
      def doStop(t: Terminable) = Throwables.quietly(s"Error stopping $t")(t.stop())
      
      terminableComponents.foreach(doStop)
    }
  }
  
  def apply(doStop: => Unit): Terminable = new Terminable {
    override def stop(): Unit = doStop
  }
}

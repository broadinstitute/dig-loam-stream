package loamstream.util

/**
 * @author clint
 * Nov 23, 2016
 */
trait Terminable {
  def stop(): Iterable[Throwable]
}

object Terminable {
  private def doStop(t: Terminable): Option[Throwable] = Throwables.failureOption(t.stop())
  
  trait StopsComponents extends Terminable { self: Loggable =>
    protected def terminableComponents: Iterable[Terminable]
    
    override def stop(): Iterable[Throwable] = terminableComponents.flatMap(doStop)
  }
  
  def apply(body: => Unit): Terminable = new Terminable {
    override def stop(): Iterable[Throwable] = Throwables.failureOption(body)
  }
}

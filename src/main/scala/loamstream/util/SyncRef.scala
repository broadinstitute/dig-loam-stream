package loamstream.util

/**
 * @author clint
 * date: Aug 4, 2016
 */
final class SyncRef[A] {
  private[this] var _value: Option[A] = None
  private[this] val lock = new AnyRef

  def value: Option[A] = lock.synchronized(_value)
  
  def value_=(newValue: Option[A]): Unit = lock.synchronized(_value = newValue)
  
  def mutate(f: A => A): Unit = lock.synchronized {
    value.map(f).foreach(update)
  }
  
  def apply(): A = value.get
  
  def update(a: A): Unit = this.value = Option(a)
  
  def getOrElse(default: => A): A = value.getOrElse(default)
}

object SyncRef {
  def apply[A](): SyncRef[A] = new SyncRef[A]
  
  def apply[A](init: A): SyncRef[A] = {
    val result = new SyncRef[A]

    result() = init

    result
  }
}
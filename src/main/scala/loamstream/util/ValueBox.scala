package loamstream.util

/** 
 * A simple container to hold a value that is being updated
 * @author clint
 * @author oliver
 * date: Aug 8, 2016 
 */
final class ValueBox[A](init: A) {

  @volatile private[this] var _value: A = init
  
  private[this] val lock = new AnyRef

  /** Returns the contained value */
  def value: A = lock.synchronized(_value)

  /** Returns the contained value */
  def apply(): A = value
  
  /** Sets a new value */
  def value_=(newValue: A): Unit = lock.synchronized {
    _value = newValue
  }

  /** Sets a new value, returns this instance */
  def update(a: A): ValueBox[A] = lock.synchronized {
    value = a
    
    this
  }
  
  /** Changes the value by applying a function to it */
  def mutate(f: A => A): ValueBox[A] = lock.synchronized {
    update(f(value))
  }

  /** Get a property of the contained value by applying a function to it */
  def get[B](g: A => B): B = lock.synchronized {
    g(value)
  }

  /** Returns an item by applying a function that also changes the contained value */
  def getAndUpdate[B](c: A => (A, B)): B = lock.synchronized {
    val (valueNew, item) = c(_value)
    
    _value = valueNew
    
    item
  }
  
  def foreach(f: A => Any): Unit = lock.synchronized {
    f(value)
  }
  
  override def toString: String = s"ValueBox($value)"
  
  override def hashCode: Int = value.hashCode
  
  override def equals(other: Any): Boolean = other match {
    case that: ValueBox[A] => this.value == that.value
    case _ => false
  }
}

object ValueBox {
  def apply[A](init: A): ValueBox[A] = new ValueBox[A](init)
  
  def unapply[A](v: ValueBox[A]): Option[A] = Option(v.value)
}

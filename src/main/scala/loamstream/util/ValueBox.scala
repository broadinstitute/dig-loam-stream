package loamstream.util

/** A simple container to hold a value that is being updated */
class ValueBox[V](valueNew: V) {

  @volatile private[this] var _value: V = valueNew

  private[this] val valueLock = new AnyRef

  /** Returns the contained value */
  def value: V = valueLock.synchronized {
    _value
  }

  /** Sets a new value */
  def value_=(newValue: V): Unit = valueLock.synchronized {
    _value = newValue
  }

  /** Changes the value by applying a function to it */
  def apply(f: V => V): ValueBox[V] = valueLock.synchronized {
    _value = f(_value)
    this
  }

  /** Get a property of the contained value by applying a function to it */
  def get[P](g: V => P): P = valueLock.synchronized {
    g(_value)
  }

  /** Returns an item by applying a function that also changes the contained value */
  def getAndUpdate[T](c: V => (V, T)): T = valueLock.synchronized {
    val (valueNew, item) = c(_value)
    _value = valueNew
    item
  }
}

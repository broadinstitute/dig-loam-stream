package loamstream.util

/** A simple container to hold a value that is being updated */
class ValueBox[V](valueNew: V) {

  @volatile private[this] var _value: V = valueNew

  /** Returns the contained value */
  def value: V = synchronized {
    _value
  }

  /** Sets a new value */
  def value_=(newValue: V): Unit = synchronized {
    _value = newValue
  }

  /** Changes the value by applying a function to it */
  def apply(f: V => V): ValueBox[V] = synchronized {
    _value = f(_value)
    this
  }

  /** Get a property of the contained value by applying a function to it */
  def get[P](g: V => P): P = synchronized {
    g(_value)
  }

  /** Returns an item by applying a function that also changes the contained value */
  def create[T](c: V => (V, T)): T = synchronized {
    val (valueNew, item) = c(_value)
    _value = valueNew
    item
  }
}

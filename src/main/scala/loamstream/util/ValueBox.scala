package loamstream.util

/** A simple container to hold a value that is being updated */
class ValueBox[V](valueNew: V) {

  @volatile private[this] var _value: V = valueNew

  def value: V = synchronized {
    _value
  }

  def value_=(newValue: V): Unit = synchronized {
    _value = newValue
  }

  def apply(f: V => V): ValueBox[V] = synchronized {
    _value = f(_value)
    this
  }
}

object ValueBox
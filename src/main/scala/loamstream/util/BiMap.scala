package loamstream.util

/**
 * @author clint
 * Nov 7, 2018
 * 
 * Represents a bi-directional one-to-one mapping between As and Bs, with unique keys and values.
 * Allows efficient lookups (constant-time, assuming the component maps offer constant-time lookups) by 
 * keys /and/ values.
 * 
 * Note: uniqueness of keys and values is guaranteed by overwriting existing mappings, just like with regular 
 * Maps.  When adding mappings with `+`, this class ensures that both forward and backward mappings
 * are overwritten if they're present.
 */
final case class BiMap[A, B] private (private[util] val forward: Map[A, B], private[util] val backward: Map[B, A]) {
  require(
      forward.size == backward.size, 
      s"Keys and values must be unique, but got forward = $forward, backward = $backward")
  
  def isEmpty: Boolean = forward.isEmpty
      
  def size: Int = forward.size
  
  def inverse: BiMap[B, A] = BiMap(backward, forward)
  
  def keys: Iterable[A] = forward.keys
  
  def values: Iterable[B] = backward.keys
  
  def get(a: A): Option[B] = forward.get(a)
  
  def getByValue(b: B): Option[A] = backward.get(b)
  
  def contains(a: A): Boolean = forward.contains(a)
  
  def containsValue(b: B): Boolean = backward.contains(b)
  
  import Maps.Implicits._
  
  def filterKeys(p: A => Boolean): BiMap[A, B] = {
    val forwardKeysToRemove = forward.keys.filterNot(p)
    
    this -- forwardKeysToRemove
  }
  
  def mapKeys[C](f: A => C): BiMap[C, B] = BiMap(forward.mapKeys(f), backward.strictMapValues(f))
  
  def +(tuple: (A, B)): BiMap[A, B] = {
    val (a, b) = tuple
    
    val withoutAnyExistingMapping = (this - a).withoutValue(b)
    
    BiMap(withoutAnyExistingMapping.forward + tuple, withoutAnyExistingMapping.backward + tuple.swap)
  }
  
  def ++(tuples: Iterable[(A, B)]): BiMap[A, B] = tuples.foldLeft(this)(_ + _)
  
  def -(a: A): BiMap[A, B] = {
    val bToRemove = forward.get(a)
    
    BiMap(forward - a, backward -- bToRemove)
  }
  
  def --(as: Iterable[A]): BiMap[A, B] = as.foldLeft(this)(_ - _)
  
  def withoutValue(b: B): BiMap[A, B] = (inverse - b).inverse
  
  def toMap: Map[A, B] = forward
}

object BiMap {
  def empty[A, B]: BiMap[A, B] = new BiMap(Map.empty, Map.empty)
  
  def apply[A, B](tuples: (A, B)*): BiMap[A, B] = new BiMap(tuples.toMap, tuples.iterator.map(_.swap).toMap)
}

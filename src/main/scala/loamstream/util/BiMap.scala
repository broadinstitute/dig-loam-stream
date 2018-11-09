package loamstream.util

/**
 * @author clint
 * Nov 7, 2018
 */
final case class BiMap[A, B] private (private[util] val forward: Map[A, B], private[util] val backward: Map[B, A]) {
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
  
  def filterKeys(p: A => Boolean): BiMap[A, B] = BiMap(forward.filterKeys(p), backward.filterValues(p))
  
  def filterValues(p: B => Boolean): BiMap[A, B] = BiMap(forward.filterValues(p), backward.filterKeys(p))
  
  def mapKeys[C](f: A => C): BiMap[C, B] = BiMap(forward.mapKeys(f), backward.strictMapValues(f))
  
  def mapValues[C](f: B => C): BiMap[A, C] = inverse.mapKeys(f).inverse
  
  def +(tuple: (A, B)): BiMap[A, B] = BiMap(forward + tuple, backward + tuple.swap)
  
  def ++(tuples: Iterable[(A, B)]): BiMap[A, B] = BiMap(forward ++ tuples, backward ++ tuples.iterator.map(_.swap))
  
  def -(a: A): BiMap[A, B] = {
    val bOpt = forward.get(a)
    
    BiMap(forward - a, backward -- bOpt)
  }
  
  def --(as: TraversableOnce[A]): BiMap[A, B] = as.foldLeft(this)(_ - _)
  
  def withoutValue(b: B): BiMap[A, B] = (inverse - b).inverse
  
  def toMap: Map[A, B] = forward
}

object BiMap {
  def empty[A, B]: BiMap[A, B] = new BiMap(Map.empty, Map.empty)
  
  def apply[A, B](tuples: (A, B)*): BiMap[A, B] = new BiMap(tuples.toMap, tuples.iterator.map(_.swap).toMap)
}

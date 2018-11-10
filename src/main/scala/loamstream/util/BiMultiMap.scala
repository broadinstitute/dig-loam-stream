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
 * Maps.  However, this can lead to situations where the 'forward' and 'backward' mappings are asymmetrical:
 *   BiMultiMap("x" -> 1, "x" -> 2)
 * This will throw; if it didn't, the un-sound BiMultiMap(forward = Map("x" -> 2), backward = Map(1 -> "x", 2 -> "x"))
 * would be produced.  When adding mappings with `+`, this class ensures that both forward and backward mappings
 * are overwritten if they're present.
 */
final case class BiMultiMap[A, B] private (
    private[util] val forward: Map[A, B], 
    private[util] val backward: Map[B, Set[A]]) {
  
  def isEmpty: Boolean = forward.isEmpty
      
  def size: Int = forward.size
  
  def keys: Iterable[A] = forward.keys
  
  def values: Iterable[B] = backward.keys
  
  def get(a: A): Option[B] = forward.get(a)
  
  def getByValue(b: B): Option[Set[A]] = backward.get(b)
  
  def contains(a: A): Boolean = forward.contains(a)
  
  def containsValue(b: B): Boolean = backward.contains(b)
  
  import Maps.Implicits._
  
  def filterKeys(p: A => Boolean): BiMultiMap[A, B] = {
    BiMultiMap(forward.filterKeys(p), backward.strictMapValues(_.filter(p)))
  }
  
  def filterValues(p: B => Boolean): BiMultiMap[A, B] = {
    BiMultiMap(forward.filterValues(p), backward.filterKeys(p))
  }
  
  def mapKeys[C](f: A => C): BiMultiMap[C, B] = BiMultiMap(forward.mapKeys(f), backward.strictMapValues(_.map(f)))
  
  def mapValues[C](f: B => C): BiMultiMap[A, C] = {
    BiMultiMap(forward.strictMapValues(f), backward.mapKeys(f))
  }
  
  def +(tuple: (A, B)): BiMultiMap[A, B] = {
    val (a, b) = tuple
    
    val newAs = backward.get(b) match {
      case Some(as) => as + a
      case None => Set(a)
    }
    
    BiMultiMap(forward + tuple, backward + (b -> newAs))
  }
  
  def ++(tuples: Iterable[(A, B)]): BiMultiMap[A, B] = tuples.foldLeft(this)(_ + _)
  
  def -(a: A): BiMultiMap[A, B] = {
    val bToRemove = forward.get(a)
    
    BiMultiMap(forward - a, (backward -- bToRemove))
  }
  
  def --(as: Iterable[A]): BiMultiMap[A, B] = {
    val bsToRemove = as.flatMap(forward.get)
    
    BiMultiMap(forward -- as, backward -- bsToRemove)
  }
  
  def withoutValue(b: B): BiMultiMap[A, B] = {
    val asToRemove = backward.getOrElse(b, Set.empty)
    
    BiMultiMap(forward -- asToRemove, backward - b)
  }
  
  def toMap: Map[A, B] = forward
}

object BiMultiMap {
  def empty[A, B]: BiMultiMap[A, B] = new BiMultiMap(Map.empty, Map.empty)
  
  def apply[A, B](tuples: (A, B)*): BiMultiMap[A, B] = {
    def addToMap[X, Y](m: Map[X, Set[Y]], tuple: (X, Y)): Map[X, Set[Y]] = {
      val (x, y) = tuple
      
      val newYs = m.get(x) match {
        case Some(ys) => ys + y
        case None => Set(y)
      }
      
      m + (x -> newYs)
    }
    
    val z = Map.empty[B, Set[A]]
    
    new BiMultiMap(tuples.toMap, tuples.iterator.map(_.swap).foldLeft(z)(addToMap))
  }
}

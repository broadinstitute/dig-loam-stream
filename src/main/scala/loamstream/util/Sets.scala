package loamstream.util

/**
 * @author clint
 * Nov 9, 2018
 */
object Sets {
  /**
   * Compute the difference between two Sets efficiently.  Converts each parameter to a HashSet if they aren't already
   * to take advantage of HashSet's faster diff() method.  This is needed because the previous approach, which used
   * `--` had O(n^2) running time.  What's more, the default implementation of `diff` for non-HashSets just calls
   * `--`, hence the need to ensure we have HashSets.  Empirically, using this method makes validation run in linear 
   * time instead of quadratic time, as it did previously.   
   */
  def hashSetDiff[A](lhs: Set[A], rhs: Set[A]): Set[A] = {
    import scala.collection.immutable.HashSet
    
    def toHashSet(s: Set[A]): HashSet[A] = s match {
      case hs: HashSet[A] => hs 
      case _ => s.to[HashSet]
    }
    
    if(lhs.isEmpty || rhs.isEmpty) { lhs }
    else { toHashSet(lhs).diff(toHashSet(rhs)) } 
  }
}

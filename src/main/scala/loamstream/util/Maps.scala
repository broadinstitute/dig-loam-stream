package loamstream.util

/**
 * @author clint
 * date: Aug 11, 2016
 */
object Maps {
  def mergeMaps[A, B](maps: TraversableOnce[Map[A, B]]): Map[A, B] = {
    val z: Map[A, B] = Map.empty

    maps.foldLeft(z)(_ ++ _)
  }
  
  object Implicits {
    implicit final class MapOps[A,B](val m: Map[A,B]) extends AnyVal {
      def strictMapValues[C](f: B => C): Map[A, C] = m.map { case (a, b) => (a, f(b)) }
      
      def mapKeys[A1](f: A => A1): Map[A1, B] = m.map { case (a, b) => (f(a), b) }
    }
  }
}
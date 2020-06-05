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
    implicit final class MapOps[A, B](val m: Map[A, B]) extends AnyVal {
      def strictMapValues[C](f: B => C): Map[A, C] = m.view.mapValues(f).toMap

      def strictFilterKeys(p: A => Boolean): Map[A, B] = m.view.filterKeys(p).toMap

      def mapKeys[A1](f: A => A1): Map[A1, B] = m.map { case (a, b) => (f(a), b) }
      
      def collectKeys[A1](pf: PartialFunction[A, A1]): Map[A1, B] = {
        m.collect { case (a, b) if pf.isDefinedAt(a) => (pf(a), b) }
      }
      
      def filterValues(p: B => Boolean): Map[A, B] = m.filter { case (_, b) => p(b) }
    }
  }
}

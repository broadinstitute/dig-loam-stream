package loamstream.util

/**
 * @author clint
 * date: Aug 11, 2016
 */
object Iterables {
  object Implicits {
    final implicit class TraversableOps[A](val as: Iterable[A]) extends AnyVal {
      def mapTo[B](field: A => B): Map[A, B] = as.map(a => a -> field(a)).toMap

      def flatMapTo[B](field: A => Iterable[B]): Map[A, B] = {
        val tuples = for {
          a <- as
          b <- field(a)
        } yield a -> b

        tuples.toMap
      }
      
      def mapBy[K](f: A => K): Map[K, A] = {
        as.map(a => f(a) -> a).toMap
      }
    }
    
    final implicit class TraversableTuple2Ops[A, B](val ts: Iterable[(A, B)]) extends AnyVal {
      
      def mapFirst[C](f: A => C): Iterable[(C, B)] = ts.map { t =>
        val (a, b) = t 
        
        (f(a), b)
      }
      
      def mapSecond[C](f: B => C): Iterable[(A, C)] = ts.map { t =>
        val (a, b) = t 
        
        (a, f(b))
      }
    }
  }
}

package loamstream.util

/**
 * @author clint
 * date: Aug 11, 2016
 */
object Traversables {
  object Implicits {
    final implicit class TraversableOps[A](val as: Traversable[A]) extends AnyVal {
      def mapTo[B](field: A => B): Map[A, B] = as.map(a => a -> field(a)).toMap

      def flatMapTo[B](field: A => Traversable[B]): Map[A, B] = {
        val tuples = for {
          a <- as
          b <- field(a)
        } yield a -> b

        tuples.toMap
      }
      
      def mapBy[K](f: A => K): Map[K, A] = {
        as.map(a => f(a) -> a).toMap
      }
      
      def splitOn(p: A => Boolean): Iterator[List[A]] = new Iterator[List[A]] {
        private val itr: Iterator[A] = as.toIterator
        
        override def hasNext: Boolean = itr.hasNext
        
        override def next(): List[A] = {
          try { itr.takeWhile(a => !p(a)).toList }
          finally { itr.dropWhile(p) }
        }
      }.filter(_.nonEmpty)
    }
    
    final implicit class TraversableTuple2Ops[A, B](val ts: Traversable[(A, B)]) extends AnyVal {
      
      def mapFirst[C](f: A => C): Traversable[(C, B)] = ts.map { t => 
        val (a, b) = t 
        
        (f(a), b)
      }
      
      def mapSecond[C](f: B => C): Traversable[(A, C)] = ts.map { t => 
        val (a, b) = t 
        
        (a, f(b))
      }
    }
  }
}

package loamstream.util

/**
 * @author clint
 * Apr 9, 2020
 */
object Iterables {
  object Implicits {
    final implicit class IterableOps[A](val as: Iterable[A]) extends AnyVal {
      def splitWhen(p: A => Boolean): Iterator[Iterable[A]] = new Iterator[Iterable[A]] {
        private val delegate = as.iterator
        private val negatedPredicate: A => Boolean = !p(_)
        
        override def hasNext: Boolean = delegate.hasNext
        
        override def next(): Seq[A] = delegate.takeWhile(negatedPredicate).toIndexedSeq
      }
    }
  }
}

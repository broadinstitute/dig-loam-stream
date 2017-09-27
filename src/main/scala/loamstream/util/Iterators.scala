package loamstream.util

/**
 * @author clint
 * Sep 25, 2017
 */
object Iterators {
  object Implicits {
    implicit final class IteratorOps[A](val itr: Iterator[A]) extends AnyVal {
      def nextOption: Option[A] = if(itr.hasNext) Some(itr.next()) else None
    }
  }
}

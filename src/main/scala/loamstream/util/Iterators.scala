package loamstream.util

/**
 * @author clint
 * Sep 25, 2017
 */
object Iterators {
  object Implicits {
    implicit final class IteratorOps[A](val itr: Iterator[A]) extends AnyVal {
      //NB: This will consume the iterator, if it's not empty
      def nextOption(): Option[A] = if(itr.hasNext) Some(itr.next()) else None
    }
  }
  
  def sample[A](as: Iterator[A], indices: Seq[Int]): Iterator[A] = {
    val indicesSet = indices.toSet

    as.zipWithIndex.collect { case (a, i) if indicesSet.contains(i) => a }
  }
}

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
  
  private[util] def deltasBetween(is: Seq[Int]): Iterator[Int] = {
    is. 
      //step through the list pairwise
      sliding(2, 1).
      //compute the difference between the numbers in each pair  
      map { case Seq(start, end) => end - start }
  }
  
  def sample[A](as: Iterator[A], indices: Seq[Int]): Iterator[A] = {
    def deltas = deltasBetween(indices.sorted)
    
    import Implicits._
    
    deltas.flatMap { d => 
      as.drop(d)
      
      as.nextOption()
    }
  }
}

package loamstream.loam.intake

import loamstream.util.Iterators

/**
 * @author clint
 * Mar 25, 2020
 */
object Sample {
  def random[A](howMany: Int)(source: Source[A]): Source[A] = {
    Source.FromIterator {
      val indices = randomIndices(source, howMany)
      
      Iterators.sample(source.records, indices)
    }
  }
  
  private[intake] def randomIndices[A](source: Source[A], howMany: Int): Seq[Int] = {
    val size = source.records.size
    
    require(howMany >= 0, s"Expected howMany to be >= 0, but got $howMany")
    
    if(size == 0 || howMany == 0) { 
      Nil
    } else {
      val r = new scala.util.Random
      
      def index() = r.nextInt(size)
      
      (1 to howMany).map(_ => index()).distinct
    }
  }
}

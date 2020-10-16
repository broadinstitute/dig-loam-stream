package loamstream.loam.intake.metrics

import loamstream.loam.intake.RowSource
import loamstream.loam.intake.CsvRow
import loamstream.util.Iterators

/**
 * @author clint
 * Mar 25, 2020
 */
object Sample {
  def random[A](howMany: Int)(source: RowSource[A]): RowSource[A] = {
    RowSource.FromIterator {
      val indices = randomIndices(source, howMany)
      
      Iterators.sample(source.records, indices)
    }
  }
  
  private[metrics] def randomIndices[A](source: RowSource[A], howMany: Int): Seq[Int] = {
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

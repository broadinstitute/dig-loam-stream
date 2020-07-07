package loamstream.loam.intake.metrics

import loamstream.loam.intake.CsvSource
import loamstream.loam.intake.CsvRow
import loamstream.util.Iterators

/**
 * @author clint
 * Mar 25, 2020
 */
object Sample {
  def random(howMany: Int)(source: CsvSource): CsvSource = {
    CsvSource.FromIterator {
      val indices = randomIndices(source, howMany)
      
      Iterators.sample(source.records, indices)
    }
  }
  
  private[metrics] def randomIndices(source: CsvSource, howMany: Int): Seq[Int] = {
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

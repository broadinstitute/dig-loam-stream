package loamstream.loam.intake


import loamstream.util.Fold


/**
 * @author clint
 * Mar 27, 2020
 */
package object metrics {
  import loamstream.loam.intake.aggregator.DataRow
  
  type Metric[A] = Fold[DataRow, _, A]
  
  implicit final class MetricOps[A](val f: Metric[A]) extends AnyVal {
    def process(rows: Source[DataRow]): A = Fold.fold(rows.records)(f)
  
    def processSampled(howMany: Int)(rows: Source[DataRow]): A = {
      Fold.fold(Sample.random(howMany)(rows).records)(f)
    }
  
    //scalastyle:off method.name
    def |+|[A2](that: Metric[A2]): Metric[(A, A2)] = Fold.combine(this.f, that.f) 
    
    def |+|[A2, B, C](that: Metric[A2])(implicit ev: A2 =:= (B, C)): Metric[(A, B, C)] = {
      Fold.combine(this.f, that.f.map(ev)).map { case (a, (b, c)) => (a, b, c) } 
    }
    
    def |+|[C, A1, B1](that: Metric[C])(implicit ev: A =:= (A1, B1), discriminator: Int = 42): Metric[(A1, B1, C)] = {
      Fold.combine(this.f.map(ev), that.f).map { case ((a, b), c) => (a, b, c) } 
    }
    //scalastyle:on method.name
    
    def combine[A2](that: Metric[A2]): Metric[(A, A2)] = Fold.combine(this.f, that.f) 
    
    /*def combine[A2, B, C](that: Metric[A2])(implicit ev: A2 =:= (B, C)): Metric[(A, B, C)] = {
      Fold.combine(this.f, that.f.map(ev)).map { case (a, (b, c)) => (a, b, c) } 
    }
    
    def combine[C, A1, B1](
        that: Metric[C])(implicit ev: A =:= (A1, B1), discriminator: Int = 42): Metric[(A1, B1, C)] = {
      Fold.combine(this.f.map(ev), that.f).map { case ((a, b), c) => (a, b, c) } 
    }*/
  }
}

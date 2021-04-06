package loamstream.loam.intake

import loamstream.util.Fold


/**
 * @author clint
 * Mar 27, 2020
 */
package object metrics {
  
  type Metric[R <: BaseVariantRow, A] = Fold[VariantRow.Parsed[R], _, A]
  
  implicit final class MetricOps[R <: BaseVariantRow, A](val f: Metric[R, A]) extends AnyVal {
    def process(rows: Source[VariantRow.Parsed[R]]): A = Fold.fold(rows.records)(f)
  
    def processSampled(howMany: Int)(rows: Source[VariantRow.Parsed[R]]): A = {
      Fold.fold(Sample.random(howMany)(rows).records)(f)
    }
  
    //scalastyle:off method.name
    def |+|[A2](that: Metric[R, A2]): Metric[R, (A, A2)] = Fold.combine(this.f, that.f) 
    
    def |+|[A2, B, C](that: Metric[R, A2])(implicit ev: A2 =:= (B, C)): Metric[R, (A, B, C)] = {
      Fold.combine(this.f, that.f.map(ev)).map { case (a, (b, c)) => (a, b, c) } 
    }
    
    def |+|[C, A1, B1](
        that: Metric[R, C])(implicit ev: A =:= (A1, B1), discriminator: Int = 0): Metric[R, (A1, B1, C)] = {
      
      Fold.combine(this.f.map(ev), that.f).map { case ((a, b), c) => (a, b, c) } 
    }
    //scalastyle:on method.name
    
    def combine[A2](that: Metric[R, A2]): Metric[R, (A, A2)] = Fold.combine(this.f, that.f) 
  }
}

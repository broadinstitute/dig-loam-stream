package loamstream.loam.intake


/**
 * @author clint
 * Mar 27, 2020
 */
package object metrics {
  type Metric[A] = Fold[CsvRow, A]
  
  implicit final class FoldOps[A](val f: Metric[A]) extends AnyVal {
    def process(rows: CsvSource): A = Fold.fold(rows.records)(f)
  
    def processSampled(howMany: Int)(rows: CsvSource): A = Fold.fold(Sample.random(howMany)(rows).records)(f)
  
    def |+|[A2](that: Metric[A2]): Metric[(A, A2)] = Fold.combine(this.f, that.f)
  }
}

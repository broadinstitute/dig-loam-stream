package loamstream.loam.intake

import scala.concurrent.Future
import scala.concurrent.ExecutionContext
import loamstream.util.Fold


/**
 * @author clint
 * Mar 27, 2020
 */
package object metrics {
  type Metric[A] = Fold[CsvRow, _, A]
  
  implicit final class MetricOps[A](val f: Metric[A]) extends AnyVal {
    def process(rows: RowSource[CsvRow]): A = Fold.fold(rows.records)(f)
  
    def processSampled(howMany: Int)(rows: RowSource[CsvRow]): A = {
      Fold.fold(Sample.random(howMany)(rows).records)(f)
    }
  
    def |+|[A2](that: Metric[A2]): Metric[(A, A2)] = Fold.combine(this.f, that.f) //scalastyle:ignore method.name
  }
}

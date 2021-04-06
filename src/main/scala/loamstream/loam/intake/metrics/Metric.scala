package loamstream.loam.intake.metrics

import loamstream.loam.intake.Variant
import loamstream.loam.intake.AggregatorVariantRow
import loamstream.loam.intake.flip.FlipDetector
import loamstream.util.Fold
import loamstream.loam.intake.DataRow
import loamstream.loam.intake.Chromosomes
import loamstream.loam.intake.flip.Disposition
import java.nio.file.Path
import loamstream.util.Files
import loamstream.loam.intake.RowSink
import loamstream.loam.intake.VariantRow


/**
 * @author clint
 * Mar 27, 2020
 */
object Metric {
  def countGreaterThan(column: VariantRow.Transformed => Double)(threshold: Double): Metric[Int] = {
    Fold.countIf[VariantRow.Parsed] {
      case _: VariantRow.Skipped => false
      case row: VariantRow.Transformed => column(row) > threshold
    }
  }
  
  def fractionGreaterThan(column: VariantRow.Transformed => Double)(threshold: Double): Metric[Double] = {
    fractionOfTotal(countGreaterThan(column)(threshold))
  }
  
  def countKnown(client: BioIndexClient): Metric[Int] = {
    countKnownOrUnknown(client)(client.isKnown)
  }
  
  def countUnknown(client: BioIndexClient): Metric[Int] = {
    countKnownOrUnknown(client)(client.isUnknown)
  }
  
  private def countKnownOrUnknown(client: BioIndexClient)(p: Variant => Boolean): Metric[Int] = {
    Fold.countIf { 
      case VariantRow.Transformed(_, dataRow) => p(dataRow.marker) 
      case _ => false
    }
  }
  
  def fractionUnknown(client: BioIndexClient): Metric[Double] = {
    fractionOfTotal(countUnknown(client))
  }
  
  private def fractionOfTotal[A](numeratorMetric: Metric[A])(implicit ev: Numeric[A]): Metric[Double] = {
    toFraction(numeratorMetric, Fold.count.map(ev.fromInt))
  }
  
  private def toFraction[A](
      numeratorMetric: Metric[A], 
      denominatorMetric: Metric[A])(implicit ev: Numeric[A]): Metric[Double] = {
    
    Fold.combine(numeratorMetric, denominatorMetric).map {
      case (numerator, denominator) => ev.toDouble(numerator) / ev.toDouble(denominator)
    }
  }
  
  def countWithDisagreeingBetaStderrZscore(epsilon: Double = 1e-8d): Metric[Int] = {
    //z = beta / se  or  -(beta / se) if flipped
    
    def agrees(expected: Double, actual: Double): Boolean = scala.math.abs(expected - actual) < epsilon
    
    def agreesNoFlip(z: Double, beta: Double, se: Double): Boolean = agrees(z, beta / se)
    
    def agreesFlip(z: Double, beta: Double, se: Double): Boolean = agrees(z, -(beta / se))
    
    def isFlipped(sourceRow: VariantRow.Tagged): Boolean = sourceRow.disposition.isFlipped
    
    val agreesFn: VariantRow.Parsed => Boolean = { 
      case VariantRow.Transformed(
          sourceRow, 
          AggregatorVariantRow(marker, _, Some(z), Some(se), Some(beta), _, _, _, _, _)) => {
            
        if(isFlipped(sourceRow)) {
          agrees(z, -(beta / se))
        } else {
          agrees(z, beta / se)
        }
      }
      case _ => true
    }
    
    def disagrees(row: VariantRow.Parsed): Boolean = !agreesFn(row)
    
    Fold.countIf(disagrees)
  }
  
  def fractionWithDisagreeingBetaStderrZscore(epsilon: Double = 1e-8d): Metric[Double] = {
    fractionOfTotal(countWithDisagreeingBetaStderrZscore(epsilon))
  }
  
  def mean[N](column: VariantRow.Transformed => N)(implicit ev: Numeric[N]): Metric[Double] = {
    val sumFold: Fold[VariantRow.Parsed, N, N] = Fold.sum {
      case _: VariantRow.Skipped => ev.zero
      case t: VariantRow.Transformed => column(t)
    }
    val countFold: Fold[VariantRow.Parsed, Int, Int] = Fold.count
    
    Fold.combine(sumFold, countFold).map {
      case (sum, count) => ev.toDouble(sum) / count.toDouble
    }
  }
  
  def countByChromosome(countSkipped: Boolean = true): Metric[Map[String, Int]] = {
    type Counts = Map[String, Int]
    
    def doAdd(acc: Counts, elem: VariantRow.Parsed): Counts = {
      if(elem.notSkipped || countSkipped) {
        import elem.derivedFrom.marker.chrom
        
        val newCount = acc.get(chrom) match {
          case Some(count) => count + 1
          case None => 1
        }
        
        acc + (chrom -> newCount)
      } else {
        acc
      }
    }
    
    def startingCounts: Counts = Map.empty ++ Chromosomes.names.iterator.map(_ -> 0) 
    
    Fold.apply[VariantRow.Parsed, Counts, Counts](startingCounts, doAdd, identity)
  }
  
  def countFlipped: Metric[Int] = Fold.countIf(_.isFlipped)
  
  def countComplemented: Metric[Int] = Fold.countIf(_.isComplementStrand)
  
  def countSkipped: Metric[Int] = Fold.countIf(_.isSkipped)
  
  def countNOTSkipped: Metric[Int] = Fold.countIf(_.notSkipped)
  
  def summaryStats: Metric[SummaryStats] = {
    val fields: Metric[(Int, Int, Int, Int, Map[String, Int])] = {
      (countNOTSkipped |+| countSkipped |+| countFlipped |+| 
       countComplemented |+| countByChromosome(countSkipped = false)).map {
        
        case ((((a, b), c), d), e) => (a, b, c, d, e) 
      }
    }
    
    fields.map(SummaryStats.tupled)
  }
  
  def writeSummaryStatsTo(file: Path): Metric[Unit] = summaryStats.map(_.toFileContents).map(Files.writeTo(file))
  
  def writeValidVariantsTo(sink: RowSink): Metric[Unit] = Fold.foreach { row =>
    if(row.notSkipped) {
      row.aggRowOpt.foreach(sink.accept)
    }
  }
}

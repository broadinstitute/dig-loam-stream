package loamstream.loam.intake.metrics

import loamstream.loam.intake.Variant
import loamstream.loam.intake.BaseVariantRow
import loamstream.loam.intake.flip.FlipDetector
import loamstream.util.Fold
import loamstream.loam.intake.DataRow
import loamstream.loam.intake.Chromosomes
import loamstream.loam.intake.flip.Disposition
import java.nio.file.Path
import loamstream.util.Files
import loamstream.loam.intake.RowSink
import loamstream.loam.intake.VariantRow
import loamstream.loam.intake.PValueVariantRow
import loamstream.loam.intake.RenderableJsonRow


/**
 * @author clint
 * Mar 27, 2020
 */
object Metric {
  def countGreaterThan[R <: BaseVariantRow](
      column: VariantRow.Parsed.Transformed[R] => Double)(threshold: Double): Metric[R, Int] = {
    
    Fold.countIf[VariantRow.Parsed[R]] {
      case VariantRow.Parsed.Skipped(_, _, _, _, _) => false
      case row @ VariantRow.Parsed.Transformed(_, _, _) => column(row) > threshold
    }
  }
  
  def fractionGreaterThan[R <: BaseVariantRow](
      column: VariantRow.Parsed.Transformed[R] => Double)(threshold: Double): Metric[R, Double] = {
    
    fractionOfTotal(countGreaterThan[R](column)(threshold))
  }
  
  def countKnown[R <: BaseVariantRow](client: BioIndexClient): Metric[R, Int] = {
    countKnownOrUnknown(client)(client.isKnown)
  }
  
  def countUnknown[R <: BaseVariantRow](client: BioIndexClient): Metric[R, Int] = {
    countKnownOrUnknown(client)(client.isUnknown)
  }
  
  private def countKnownOrUnknown[R <: BaseVariantRow](
      client: BioIndexClient)(p: Variant => Boolean): Metric[R, Int] = {
    
    Fold.countIf { 
      case VariantRow.Parsed.Transformed(_, _, dataRow) => p(dataRow.marker) 
      case _ => false
    }
  }
  
  def fractionUnknown[R <: BaseVariantRow](client: BioIndexClient): Metric[R, Double] = {
    fractionOfTotal(countUnknown(client))
  }
  
  private def fractionOfTotal[R <: BaseVariantRow, A](
      numeratorMetric: Metric[R, A])(implicit ev: Numeric[A]): Metric[R, Double] = {
    
    toFraction(numeratorMetric, Fold.count.map(ev.fromInt))
  }
  
  private def toFraction[R <: BaseVariantRow, A](
      numeratorMetric: Metric[R, A], 
      denominatorMetric: Metric[R, A])(implicit ev: Numeric[A]): Metric[R, Double] = {
    
    Fold.combine(numeratorMetric, denominatorMetric).map {
      case (numerator, denominator) => ev.toDouble(numerator) / ev.toDouble(denominator)
    }
  }
  
  private object WithMarkerZSeBeta {
    def unapply[R <: BaseVariantRow](
        avr: BaseVariantRow): Option[(Variant, Option[Double], Option[Double], Option[Double])] = avr match {
      
      case vr: PValueVariantRow => Some((vr.marker, vr.zscore, vr.stderr, vr.beta))
      case _ => None
    }
  }
  
  def countWithDisagreeingBetaStderrZscore[R <: BaseVariantRow](epsilon: Double = 1e-8d): Metric[R, Int] = {
    //z = beta / se  or  -(beta / se) if flipped
    
    def agrees(expected: Double, actual: Double): Boolean = scala.math.abs(expected - actual) < epsilon
    
    def agreesNoFlip(z: Double, beta: Double, se: Double): Boolean = agrees(z, beta / se)
    
    def agreesFlip(z: Double, beta: Double, se: Double): Boolean = agrees(z, -(beta / se))
    
    def isFlipped(sourceRow: VariantRow.Analyzed.Tagged): Boolean = sourceRow.disposition.isFlipped
    
    val agreesFn: VariantRow.Parsed[R] => Boolean = { 
      case tr @ VariantRow.Parsed.Transformed(
          _, 
          _, 
          WithMarkerZSeBeta(marker, Some(z), Some(se), Some(beta))) => {
            
        if(isFlipped(tr.derivedFromTagged)) {
          agrees(z, -(beta / se))
        } else {
          agrees(z, beta / se)
        }
      }
      case _ => true
    }
    
    def disagrees(row: VariantRow.Parsed[R]): Boolean = !agreesFn(row)
    
    Fold.countIf(disagrees)
  }
  
  def fractionWithDisagreeingBetaStderrZscore[R <: BaseVariantRow](epsilon: Double = 1e-8d): Metric[R, Double] = {
    fractionOfTotal(countWithDisagreeingBetaStderrZscore(epsilon))
  }
  
  def mean[R <: BaseVariantRow, N](
      column: VariantRow.Parsed.Transformed[R] => N)(implicit ev: Numeric[N]): Metric[R, Double] = {
    
    val sumFold: Fold[VariantRow.Parsed[R], N, N] = Fold.sum {
      case VariantRow.Parsed.Skipped(_, _, _, _, _) => ev.zero
      case t @ VariantRow.Parsed.Transformed(_, _, _) => column(t)
    }
    val countFold: Fold[VariantRow.Parsed[R], Int, Int] = Fold.count
    
    Fold.combine(sumFold, countFold).map {
      case (sum, count) => ev.toDouble(sum) / count.toDouble
    }
  }
  
  def countByChromosome[R <: BaseVariantRow](countSkipped: Boolean = true): Metric[R, Map[String, Int]] = {
    type Counts = Map[String, Int]
    
    def doAdd(acc: Counts, elem: VariantRow.Parsed[R]): Counts = {
      elem.derivedFromAnalyzed match {
        // we can't count this kind of skipped row, since it was skipped before a marker 
        // was computed from it, so we can't know which chromosome to assign the count to.
        case Some(tagged: VariantRow.Analyzed.Tagged) => {
          if(tagged.notSkipped || countSkipped) {
            import tagged.marker.chrom
            
            val newCount = acc.get(chrom) match {
              case Some(count) => count + 1
              case None => 1
            }
            
            acc + (chrom -> newCount)
          } else {
            acc
          }
        }
        case _ => acc
      }
    }
    
    def startingCounts: Counts = Map.empty ++ Chromosomes.names.iterator.map(_ -> 0) 
    
    Fold.apply[VariantRow.Parsed[R], Counts, Counts](startingCounts, doAdd, identity)
  }
  
  def countFlipped[R <: BaseVariantRow]: Metric[R, Int] = Fold.countIf {
    case t: VariantRow.Parsed.Transformed[R] => t.isFlipped
    case _ => false
  }
  
  def countComplemented[R <: BaseVariantRow]: Metric[R, Int] = Fold.countIf {
    case t: VariantRow.Parsed.Transformed[R] => t.isComplementStrand
    case _ => false
  }
  
  def countSkipped[R <: BaseVariantRow]: Metric[R, Int] = Fold.countIf(_.isSkipped)
  
  def countNOTSkipped[R <: BaseVariantRow]: Metric[R, Int] = Fold.countIf(_.notSkipped)
  
  def summaryStats[R <: BaseVariantRow]: Metric[R, SummaryStats] = {
    val fields: Metric[R, (Int, Int, Int, Int, Map[String, Int])] = {
      (countNOTSkipped[R] |+| countSkipped[R] |+| countFlipped[R] |+| 
       countComplemented[R] |+| countByChromosome[R](countSkipped = false)).map {
        
        case ((((a, b), c), d), e) => (a, b, c, d, e) 
      }
    }
    
    fields.map(SummaryStats.tupled)
  }
  
  def writeSummaryStatsTo[R <: BaseVariantRow](file: Path): Metric[R, Unit] = {
    summaryStats.map(_.toFileContents).map(Files.writeTo(file))
  }
  
  def writeValidVariantsTo[R <: BaseVariantRow](
      sink: RowSink[R]): Metric[R, Unit] = Fold.foreach { row =>
    if(row.notSkipped) {
      row.aggRowOpt.foreach(sink.accept)
    }
  }
}

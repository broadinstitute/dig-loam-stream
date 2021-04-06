package loamstream.loam.intake.metrics

import loamstream.model.Store
import loamstream.util.Files
import loamstream.util.Fold
import loamstream.loam.intake.flip.FlipDetector
import loamstream.loam.intake.BaseVariantRow

/**
 * @author clint
 * Oct 21, 2020
 */
trait Metrics {
  object Metrics {
    def count[R <: BaseVariantRow](dest: Store): Metric[R, Unit] = {
      require(dest.isPathStore)
      
      Fold.count.map(c => Files.writeTo(dest.path)(s"Produced ${c} variants"))
    }
    
    def fractionUnknownToBioIndex[R <: BaseVariantRow](
        dest: Store, 
        client: BioIndexClient = new BioIndexClient.Default()): Metric[R, Unit] = {
      
      require(dest.isPathStore)
      
      val count: Metric[R, Int] = Fold.count
      
      val percentUnknown: Metric[R, Double] = Metric.fractionUnknown(client).map(_ * 100.0)
      
      (count.combine(percentUnknown)).map {
        case (c, p) => Files.writeTo(dest.path)(s"Out of ${c} variants, ${p}% were unknown to the BioIndex")
      }
    }
    
    def fractionWithDisagreeingBetaStderrZscore[R <: BaseVariantRow](
        dest: Store, 
        flipDetector: FlipDetector): Metric[R, Unit] = {
      
      require(dest.isPathStore)
      
      val count: Metric[R, Int] = Fold.count
      
      val percentDisagreeing: Metric[R, Double] = {
        Metric.fractionWithDisagreeingBetaStderrZscore().map(_ * 100.0)
      }
      
      (count.combine(percentDisagreeing)).map {
        case (c, p) => 
          Files.writeTo(dest.path)(s"Out of ${c} variants, ${p}% had disagreeing z, stderr, and beta values")
      }
    }
    
    def writeSummaryStatsTo[R <: BaseVariantRow](dest: Store): Metric[R, Unit] = {
      require(dest.isPathStore)
      
      Metric.writeSummaryStatsTo(dest.path)
    }
  }
}

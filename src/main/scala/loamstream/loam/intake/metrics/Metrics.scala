package loamstream.loam.intake.metrics

import loamstream.model.Store
import loamstream.util.Files
import loamstream.util.Fold
import loamstream.loam.intake.flip.FlipDetector

/**
 * @author clint
 * Oct 21, 2020
 */
trait Metrics {
  object Metrics {
    def fractionUnknownToBioIndex(dest: Store, client: BioIndexClient = new BioIndexClient.Default()): Metric[Unit] = {
      require(dest.isPathStore)
      
      val count: Metric[Int] = Fold.count
      
      val percentUnknown: Metric[Double] = Metric.fractionUnknown(client).map(_ * 100.0)
      
      (count.combine(percentUnknown)).map {
        case (c, p) => Files.writeTo(dest.path)(s"Out of ${c} variants, ${p}% were unknown to the BioIndex")
      }
    }
    
    def fractionWithDisagreeingBetaStderrZscore(dest: Store, flipDetector: FlipDetector): Metric[Unit] = {
      require(dest.isPathStore)
      
      val count: Metric[Int] = Fold.count
      
      val percentDisagreeing: Metric[Double] = {
        Metric.fractionWithDisagreeingBetaStderrZscore(flipDetector).map(_ * 100.0)
      }
      
      (count.combine(percentDisagreeing)).map {
        case (c, p) => 
          Files.writeTo(dest.path)(s"Out of ${c} variants, ${p}% had disagreeing z, stderr, and beta values")
      }
    }
  }
}

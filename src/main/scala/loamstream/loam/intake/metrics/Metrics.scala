/*package loamstream.loam.intake.metrics

import loamstream.loam.intake.ColumnName
import loamstream.loam.intake.CsvSource
import java.nio.file.Paths
import loamstream.util.TimeUtils

object Metrics extends App {
  val marker = ColumnName("marker")
  val eaf = ColumnName("eaf")
  
  val gt5 = Metric.fractionGreaterThan(eaf)(0.5)
  val gt9 = Metric.fractionGreaterThan(eaf)(0.9)
  
  val gt = gt5 |+| gt9
  
  val unknown = Metric.fractionUnknown(marker, new BioIndexClient.Default())
  
  def source = CsvSource.fromFile(Paths.get("ready-for-intake-PC1diet.tsv"))
  
  val (fracGtPoint5, fracGtPoint9) = TimeUtils.time("Computing fractions greater than thresholds", println(_)) {
    gt.process(source)
  }
  
  val fracUnknown = TimeUtils.time("Computing fraction unknown", println(_)) {
    unknown.processSampled(1000)(source)
  }
  
  println(s"Fraction > 0.5: $fracGtPoint5")
  println(s"Fraction > 0.9: $fracGtPoint9")
  println(s"Fraction unknown: $fracUnknown")
}
*/

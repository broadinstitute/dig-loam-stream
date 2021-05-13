package loamstream.loam.intake.metrics

import scala.collection.compat._

final case class SummaryStats(
    validVariants: Int, //valid, ie, not skipped
    skippedVariants: Int,
    flippedVariants: Int,
    complementedVariants: Int,
    validVariantsByChromosome: Map[String, Int]) {
  
  def totalVariants: Int = validVariants + skippedVariants
  
  private def toPercent(ratio: Double): Double = ratio * 100.0
  
  private def asFraction(count: Int): Double = count.toDouble / totalVariants.toDouble
  
  def fractionValid: Double = asFraction(validVariants)
  
  def percentValid: Double = toPercent(fractionValid)
  
  def fractionSkipped: Double = asFraction(skippedVariants)
  
  def percentSkipped: Double = toPercent(fractionSkipped)
  
  def fractionFlipped: Double = asFraction(flippedVariants)
  
  def percentFlipped: Double = toPercent(fractionFlipped)
  
  def fractionComplemented: Double = asFraction(complementedVariants)
  
  def percentComplemented: Double = toPercent(fractionComplemented)
  
  def chromosomesWithNoValidVariants: Set[String] = {
    validVariantsByChromosome.collect { case (chrom, 0) => chrom }.to(Set)
  }
  
  def toFileContents: String = {
    def padChromName(chrom: String): String = chrom match {
      case "X" => "23"
      case "Y" => "24"
      case "XY" => "25"
      case "MT" => "26"
      case _ => chrom.size match {
        case 1 => s"0${chrom}"
        case _ => chrom
      }
    }
    
    def paddedChromName(t: (String, Int)): String = t match {
      case (chrom, count) => padChromName(chrom)
    }
    
    def tupleToString(t: (String, Int)): String = t match {
      case (chrom, count) => s"  $chrom: $count"
    }
    
s"""
Total variants: ${totalVariants}
Valid: ${validVariants} (${percentValid} %)
Skipped: ${skippedVariants} (${percentSkipped} %)
Flipped: ${flippedVariants} (${percentFlipped} %)
Complemented: ${complementedVariants} (${percentComplemented} %)
Counts by chromosome: 
${validVariantsByChromosome.to(Seq).sortBy(paddedChromName).map(tupleToString).mkString(System.lineSeparator)}
Chromosomes with no variants: ${chromosomesWithNoValidVariants.to(Seq).sortBy(padChromName).mkString(",")}
""".stripMargin.trim
  }
}

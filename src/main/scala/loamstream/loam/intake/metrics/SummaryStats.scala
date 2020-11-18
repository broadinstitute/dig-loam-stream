package loamstream.loam.intake.metrics

final case class SummaryStats(
    validVariants: Int,
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
    validVariantsByChromosome.collect { case (chrom, 0) => chrom }.toSet
  }
  
  def toFileContents: String = {
    def padChromName(chrom: String): String = chrom.size match {
      case 1 => s"00${chrom}"
      case 2 => s"0${chrom}"
      case _ => chrom
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
${validVariantsByChromosome.toSeq.sortBy(paddedChromName).map(tupleToString).mkString(System.lineSeparator)}
Chromosomes with no variants: ${chromosomesWithNoValidVariants.toSeq.sortBy(padChromName).mkString(",")}
""".stripMargin.trim
  }
}

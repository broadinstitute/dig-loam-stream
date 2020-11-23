package loamstream.loam.intake.metrics

import org.scalatest.FunSuite
import loamstream.loam.intake.Chromosomes

/**
 * @author clint
 * Nov 23, 2020
 */
final class SummaryStatsTest extends FunSuite {
  test("toFileContents") {
    val counts: Map[String, Int] = Chromosomes.names.map(_ -> 0).toMap ++ 
      (1 to 8).map(_.toString).map(_ -> 1) ++
      Seq("9" -> 2)
    
    val stats = SummaryStats(
        validVariants = 8,
        skippedVariants = 2,
        flippedVariants = 1,
        complementedVariants = 2,
        validVariantsByChromosome = counts)
        
    val expected = {
      s"""|Total variants: 10
          |Valid: 8 (80.0 %)
          |Skipped: 2 (20.0 %)
          |Flipped: 1 (10.0 %)
          |Complemented: 2 (20.0 %)
          |Counts by chromosome: 
          |  1: 1
          |  2: 1
          |  3: 1
          |  4: 1
          |  5: 1
          |  6: 1
          |  7: 1
          |  8: 1
          |  9: 2
          |  10: 0
          |  11: 0
          |  12: 0
          |  13: 0
          |  14: 0
          |  15: 0
          |  16: 0
          |  17: 0
          |  18: 0
          |  19: 0
          |  20: 0
          |  21: 0
          |  22: 0
          |  X: 0
          |  Y: 0
          |  XY: 0
          |  MT: 0
          |Chromosomes with no variants: 10,11,12,13,14,15,16,17,18,19,20,21,22,X,Y,XY,MT""".stripMargin.trim
    }
    
    assert(stats.toFileContents.trim === expected)
  }
}

package loamstream.loam.intake

import org.scalatest.FunSuite

/**
 * @author clint
 * Nov 6, 2020
 */
final class DataRowTest extends FunSuite {
  private val marker: Variant = Variant.from("3_456_T_C")
  private val pvalue: Double = 1.23
  private val zscore: Double = 4.56
  private val stderr: Double = 7.89
  private val beta: Double = 10.11
  private val oddsRatio: Double = 12.13
  private val eaf: Double = 13.14
  private val maf: Double = 14.15
  private val n: Double = 16.17
  
  test("values - all fields supplied") {
    val row = DataRow(
      marker = marker,
      pvalue = pvalue,
      zscore = Some(zscore),
      stderr = Some(stderr),
      beta = Some(beta),
      oddsRatio = Some(oddsRatio),
      eaf = Some(eaf),
      maf = Some(maf),
      n = Some(n))
      
    val expected = Seq(
      marker.underscoreDelimited,
      pvalue,
      zscore,
      stderr,
      beta,
      oddsRatio,
      eaf,
      maf,
      n).map(_.toString)
      
    assert(row.values === expected)
  }
  
  test("values - some fields supplied") {
    val row = DataRow(
      marker = marker,
      pvalue = pvalue,
      zscore = Some(zscore),
      stderr = None,
      beta = Some(beta),
      oddsRatio = None,
      eaf = Some(eaf),
      maf = None,
      n = Some(n))
      
    val expected = Seq(
      marker.underscoreDelimited,
      pvalue,
      zscore,
      beta,
      eaf,
      n).map(_.toString)
      
    assert(row.values === expected)
  }
}

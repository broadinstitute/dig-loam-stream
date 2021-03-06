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
  private val dataset: String = "asjhkasdfjksdhf"
  private val phenotype: String = "jiguhsfkjghksfgh"
  private val ancestry: Ancestry = Ancestry.AA
  
  test("values - all fields supplied") {
    val row = PValueVariantRow(
      marker = marker,
      pvalue = pvalue,
      dataset = dataset,
      phenotype = phenotype,
      ancestry = ancestry,
      zscore = Some(zscore),
      stderr = Some(stderr),
      beta = Some(beta),
      oddsRatio = Some(oddsRatio),
      eaf = Some(eaf),
      maf = Some(maf),
      n = n)
      
    val expected = Seq(
      marker.underscoreDelimited,
      pvalue,
      zscore,
      stderr,
      beta,
      oddsRatio,
      eaf,
      maf,
      n).map(_.toString).map(Option(_))
      
    assert(row.values === expected)
  }
  
  test("values - some fields supplied") {
    val row = PValueVariantRow(
      marker = marker,
      pvalue = pvalue,
      dataset = dataset,
      phenotype = phenotype,
      ancestry = ancestry,
      zscore = Some(zscore),
      stderr = None,
      beta = Some(beta),
      oddsRatio = None,
      eaf = Some(eaf),
      maf = None,
      n = n)
      
    val expected: Seq[Option[String]] = Seq(
      Some(marker.underscoreDelimited),
      Some(pvalue.toString),
      Some(zscore.toString),
      None,
      Some(beta.toString),
      None,
      Some(eaf.toString),
      None,
      Some(n.toString))
      
    assert(row.values === expected)
  }
}

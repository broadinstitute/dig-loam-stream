package loamstream.loam.intake

import org.scalatest.FunSuite

/**
 * @author clint
 * May 4, 2021
 */
final class VariantCountRowTest extends FunSuite {
  private val variant = Variant("1", 2, "A", "C")
  
  test("jsonValues - optional values present") {
    import org.json4s._
    
    val row = new VariantCountRow(
      marker = variant,
      dataset = "some-ds",
      phenotype = "some-pheno",
      ancestry = Ancestry.AA,
      alleleCount = Some(9),
      alleleCountCases = Some(8), 
      alleleCountControls = Some(7),
      heterozygousCases = Some(6), 
      heterozygousControls = Some(5), 
      homozygousCases = Some(4), 
      homozygousControls = Some(3), 
      derivedFromRecordNumber = Some(42))
      
    val expectedJson = Seq(
      "varId" -> JString("1:2:A:C"),
      "chromosome" -> JString("1"), 
      "position" -> JLong(2),
      "reference" -> JString("A"),
      "alt" -> JString("C"),
      "multiAllelic" -> JBool(false),
      "dataset" -> JString("some-ds"),
      "phenotype" -> JString("some-pheno"),
      "ancestry" -> JString("AA"),
      "alleleCount" -> JLong(9),
      "alleleCountCases" -> JLong(8), 
      "alleleCountControls" -> JLong(7),
      "heterozygousCases" -> JLong(6), 
      "heterozygousControls" -> JLong(5), 
      "homozygousCases" -> JLong(4), 
      "homozygousControls" -> JLong(3)) 
    
    val json = row.jsonValues
    
    assert(json === expectedJson)
  }
  
  test("jsonValues - optional values missing") {
    import org.json4s._
    
    val row = new VariantCountRow(
      marker = variant,
      dataset = "some-ds",
      phenotype = "some-pheno",
      ancestry = Ancestry.AA,
      alleleCount = None,
      alleleCountCases = None, 
      alleleCountControls = None,
      heterozygousCases = None, 
      heterozygousControls = None, 
      homozygousCases = None, 
      homozygousControls = None, 
      derivedFromRecordNumber = None)
      
    val expectedJson = Seq(
      "varId" -> JString("1:2:A:C"),
      "chromosome" -> JString("1"), 
      "position" -> JLong(2),
      "reference" -> JString("A"),
      "alt" -> JString("C"),
      "multiAllelic" -> JBool(false),
      "dataset" -> JString("some-ds"),
      "phenotype" -> JString("some-pheno"),
      "ancestry" -> JString("AA"),
      "alleleCount" -> JNull,
      "alleleCountCases" -> JNull, 
      "alleleCountControls" -> JNull,
      "heterozygousCases" -> JNull, 
      "heterozygousControls" -> JNull, 
      "homozygousCases" -> JNull, 
      "homozygousControls" -> JNull) 
    
    val json = row.jsonValues
    
    assert(json === expectedJson)
  }
  
  test("headers") {
    val row = new VariantCountRow(
      marker = variant,
      dataset = "some-ds",
      phenotype = "some-pheno",
      ancestry = Ancestry.AA,
      alleleCount = None,
      alleleCountCases = None, 
      alleleCountControls = None,
      heterozygousCases = None, 
      heterozygousControls = None, 
      homozygousCases = None, 
      homozygousControls = None, 
      derivedFromRecordNumber = None)
    
    assert(row.headers === BaseVariantRow.Headers.forVariantCountData)
    
    val expectedHeaders: Seq[String] = Seq(
      "marker",
      "dataset",
      "phenotype",
      "ancestry",
      "alleleCount",
      "alleleCountCases", 
      "alleleCountControls",
      "heterozygousCases", 
      "heterozygousControls", 
      "homozygousCases", 
      "homozygousControls")
    
    assert(row.headers === expectedHeaders)
    assert(BaseVariantRow.Headers.forVariantCountData === expectedHeaders)
  }
  
  test("values - optional values present") {
    val row = new VariantCountRow(
      marker = variant,
      dataset = "some-ds",
      phenotype = "some-pheno",
      ancestry = Ancestry.AA,
      alleleCount = Some(9),
      alleleCountCases = Some(8), 
      alleleCountControls = Some(7),
      heterozygousCases = Some(6), 
      heterozygousControls = Some(5), 
      homozygousCases = Some(4), 
      homozygousControls = Some(3), 
      derivedFromRecordNumber = Some(42))
    
    val expectedValues: Seq[Option[String]] = Seq(
      Some("1_2_A_C"),
      Some("some-ds"),
      Some("some-pheno"),
      Some("AA"),
      Some("9"),
      Some("8"),
      Some("7"),
      Some("6"),
      Some("5"),
      Some("4"),
      Some("3"))
    
    assert(row.values === expectedValues)
  }
  
  test("values - optional values missing") {
    val row = new VariantCountRow(
      marker = variant,
      dataset = "some-ds",
      phenotype = "some-pheno",
      ancestry = Ancestry.AA,
      alleleCount = None,
      alleleCountCases = None, 
      alleleCountControls = None,
      heterozygousCases = None, 
      heterozygousControls = None, 
      homozygousCases = None, 
      homozygousControls = None, 
      derivedFromRecordNumber = None)
    
    val expectedValues: Seq[Option[String]] = Seq(
      Some("1_2_A_C"),
      Some("some-ds"),
      Some("some-pheno"),
      Some("AA"),
      None,
      None,
      None,
      None,
      None,
      None,
      None)
    
    assert(row.values === expectedValues)
  }
}
package loamstream.loam.intake

import org.scalatest.FunSuite

/**
 * @author clint
 * May 4, 2021
 */
final class PValueVariantRowTest extends FunSuite {
  private val variant = Variant.from("1", 2, "A", "C")
  
  test("jsonValues - optional values present") {
    import org.json4s._
    
    final class VariantSerializer extends CustomSerializer[Variant] (
      _ => ({ case JString(s) => Variant(s) }, 
      { case v: Variant => JString(v.underscoreDelimited) })
    )

    final class AncestrySerializer extends CustomSerializer[Ancestry] (
      _ => ({ case JString(s) => Ancestry.tryFromString(s).get }, 
      { case a: Ancestry=> JString(a.name) })
    )
    
    val ser = FieldSerializer[PValueVariantRow](FieldSerializer.ignore("derivedFromRecordNumber"))
    
    val row = new PValueVariantRow(
      marker = variant,
      pvalue = 1.23,
      dataset = "some-ds",
      phenotype = "some-pheno",
      ancestry = Ancestry.AA,
      zscore = Some(4.56),
      stderr = Some(7.89),
      beta = Some(3.21),
      oddsRatio = Some(6.54),
      eaf = Some(99.0),
      maf = Some(42.0),
      n = 123.456,
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
      "pValue" -> JDouble(1.23),
      "beta" -> JDouble(3.21),
      "oddsRatio" -> JDouble(6.54),
      "eaf" -> JDouble(99.0),
      "maf" -> JDouble(42.0),
      "stdErr" -> JDouble(7.89),
      "zScore" -> JDouble(4.56),
      "n" -> JDouble(123.456)) 
    
    val json = row.jsonValues
    
    assert(json === expectedJson)
  }
  
  test("jsonValues - optional values missing") {
    import org.json4s._
    
    val row = new PValueVariantRow(
      marker = variant,
      pvalue = 1.23,
      dataset = "some-ds",
      phenotype = "some-pheno",
      ancestry = Ancestry.AA,
      zscore = None,
      stderr = None,
      beta = None,
      oddsRatio = None,
      eaf = None,
      maf = None,
      n = 123.456,
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
      "pValue" -> JDouble(1.23),
      "beta" -> JNull,
      "oddsRatio" -> JNull,
      "eaf" -> JNull,
      "maf" -> JNull,
      "stdErr" -> JNull,
      "zScore" -> JNull,
      "n" -> JDouble(123.456)) 
    
    val json = row.jsonValues
    
    assert(json === expectedJson)
  }
  
  test("headers") {
    val row = new PValueVariantRow(
      marker = variant,
      pvalue = 1.23,
      dataset = "some-ds",
      phenotype = "some-pheno",
      ancestry = Ancestry.AA,
      zscore = None,
      stderr = None,
      beta = None,
      oddsRatio = None,
      eaf = None,
      maf = None,
      n = 123.456,
      derivedFromRecordNumber = None)
    
    assert(row.headers === BaseVariantRow.Headers.forVariantData)
    
    val expectedHeaders: Seq[String] = Seq(
      "marker",
      "dataset",
      "phenotype",
      "ancestry",
      "pvalue",
      "zscore",
      "stderr",
      "beta",
      "odds_ratio",
      "eaf",
      "maf",
      "n")
    
    assert(row.headers === expectedHeaders)
    assert(BaseVariantRow.Headers.forVariantData === expectedHeaders)
  }
  
  test("values - optional values present") {
    val row = new PValueVariantRow(
      marker = variant,
      pvalue = 1.23,
      dataset = "some-ds",
      phenotype = "some-pheno",
      ancestry = Ancestry.AA,
      zscore = Some(4.56),
      stderr = Some(7.89),
      beta = Some(3.21),
      oddsRatio = Some(6.54),
      eaf = Some(99.0),
      maf = Some(42.0),
      n = 123.456,
      derivedFromRecordNumber = Some(42))
    
    val expectedValues: Seq[Option[String]] = Seq(
      Some("1_2_A_C"),
      Some("some-ds"),
      Some("some-pheno"),
      Some("AA"),
      Some("1.23"),
      Some("4.56"),
      Some("7.89"),
      Some("3.21"),
      Some("6.54"),
      Some("99.0"),
      Some("42.0"),
      Some("123.456"))
    
    assert(row.values === expectedValues)
  }
  
  test("values - optional values missing") {
    val row = new PValueVariantRow(
      marker = variant,
      pvalue = 1.23,
      dataset = "some-ds",
      phenotype = "some-pheno",
      ancestry = Ancestry.AA,
      zscore = None,
      stderr = None,
      beta = None,
      oddsRatio = None,
      eaf = None,
      maf = None,
      n = 123.456,
      derivedFromRecordNumber = Some(42))
    
    val expectedValues: Seq[Option[String]] = Seq(
      Some("1_2_A_C"),
      Some("some-ds"),
      Some("some-pheno"),
      Some("AA"),
      Some("1.23"),
      None,
      None,
      None,
      None,
      None,
      None,
      Some("123.456"))
    
    assert(row.values === expectedValues)
  }
}
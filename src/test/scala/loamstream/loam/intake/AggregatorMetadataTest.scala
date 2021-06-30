package loamstream.loam.intake

import scala.collection.immutable.Seq

import org.scalatest.FunSuite


/**
 * @author clint
 * Jun 25, 2020
 */
final class AggregatorMetadataTest extends FunSuite {
  test("escape") {
    import AggregatorMetadata.escape
    
    assert(escape("") === "")
    assert(escape("foo") === "foo")
    assert(escape("foo bar") === "\"foo bar\"")
    assert(escape(" ") === "\" \"")
    
    assert(escape("foo_\"lalala\"") === "foo_\"lalala\"")
    assert(escape("foo \"lalala\"") === "\"foo \\\"lalala\\\"\"")
  }
  
  test("asConfigFileContents") {
    import System.lineSeparator
    
    def doTest(
        author: Option[String], 
        expectedAuthorString: String,
        quantitative: Option[AggregatorMetadata.Quantitative], 
        expectedQuantString: String): Unit = {

      val dataset = "some-dataset"
      val phenotype = "some-phenotype"
      val ancestry = Ancestry.AA
      val tech = TechType.ExChip
      val properties = Seq("x" -> "123", "a" -> "456")
    
      val m = AggregatorMetadata(
        bucketName = "some-bucket",
        topic = Option(UploadType.Variants),
        dataset = dataset,
        phenotype = phenotype,
        ancestry = ancestry,
        author = author,
        tech = tech,
        quantitative = quantitative,
        properties = properties)

      val expected = s"""
        |uri s3://some-bucket/variants/ExChip/some-dataset/some-phenotype
        |dataset ${dataset} ${phenotype}
        |ancestry ${ancestry}
        |tech ${tech}
        |${expectedQuantString}
        |${expectedAuthorString}""".stripMargin.trim
    
      def removeEmptyLines(s: String): String = {
        s.split(lineSeparator).iterator.map(_.trim).filter(_.nonEmpty).mkString(lineSeparator)
      }
        
      //NB: Ignore empty lines when considering differences
      assert(removeEmptyLines(m.asMetadataFileContents) === removeEmptyLines(expected))
    }
    
    doTest(None, "", None, "")
    doTest(Some("some-author"), "author some-author", None, "")
    doTest(None, "", Some(AggregatorMetadata.Quantitative.Subjects(42)), "subjects 42")
    doTest(Some("some-author"), 
        "author some-author", Some(AggregatorMetadata.Quantitative.Subjects(42)), "subjects 42")
    doTest(
        None, 
        "", 
        Some(AggregatorMetadata.Quantitative.CasesAndControls(42, 99)), 
        s"cases 42${lineSeparator}controls 99")
    doTest(
        Some("some-author"), 
        "author some-author",
        Some(AggregatorMetadata.Quantitative.CasesAndControls(42, 99)), 
        s"cases 42${lineSeparator}controls 99")
  }
}

package loamstream.loam.intake

import org.scalatest.FunSuite
import org.scalactic.source.Position.apply
import scala.collection.Seq

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
      val varIdFormat = "{alt}_{ref}_{pos}_{chrom}"
      val ancestry = "some ancestry"
      val tech = "some tech"
      val properties = Seq("x" -> "123", "a" -> "456")
    
      val m = AggregatorMetadata(
        dataset = dataset,
        phenotype = phenotype,
        varIdFormat = varIdFormat,
        ancestry = ancestry,
        author = author,
        tech = tech,
        quantitative = quantitative,
        properties = properties)

      val expected = s"""
        |dataset ${dataset} ${phenotype}
        |ancestry "${ancestry}"
        |tech "${tech}"
        |${expectedQuantString}
        |var_id ${varIdFormat}
        |${expectedAuthorString}""".stripMargin.trim
    
      def removeEmptyLines(s: String): String = {
        s.split(lineSeparator).iterator.map(_.trim).filter(_.nonEmpty).mkString(lineSeparator)
      }
        
      //NB: Ignore empty lines when considering differences
      assert(removeEmptyLines(m.asConfigFileContents) === removeEmptyLines(expected))
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

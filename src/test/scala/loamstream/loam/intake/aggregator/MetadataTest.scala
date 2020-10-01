package loamstream.loam.intake.aggregator

import org.scalatest.FunSuite

/**
 * @author clint
 * Jun 25, 2020
 */
final class MetadataTest extends FunSuite {
  test("escape") {
    import Metadata.escape
    
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
        quantitative: Option[Metadata.Quantitative], 
        expectedQuantString: String): Unit = {

      val dataset = "some-dataset"
      val phenotype = "some-phenotype"
      val varIdFormat = "{alt}_{ref}_{pos}_{chrom}"
      val ancestry = "some ancestry"
      val tech = "some tech"
      val properties = Seq("x" -> "123", "a" -> "456")
    
      val m = Metadata(
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
    doTest(None, "", Some(Metadata.Quantitative.Subjects(42)), "subjects 42")
    doTest(Some("some-author"), "author some-author", Some(Metadata.Quantitative.Subjects(42)), "subjects 42")
    doTest(
        None, 
        "", 
        Some(Metadata.Quantitative.CasesAndControls(42, 99)), 
        s"cases 42${lineSeparator}controls 99")
    doTest(
        Some("some-author"), 
        "author some-author",
        Some(Metadata.Quantitative.CasesAndControls(42, 99)), 
        s"cases 42${lineSeparator}controls 99")
  }
}

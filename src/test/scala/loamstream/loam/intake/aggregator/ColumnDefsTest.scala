package loamstream.loam.intake.aggregator

import org.scalatest.FunSuite
import loamstream.loam.intake.ColumnDef
import loamstream.loam.intake.ColumnName
import loamstream.loam.intake.flip.FlipDetector
import loamstream.loam.intake.Helpers

/**
 * @author clint
 * Sep 28, 2020
 */
final class ColumnDefsTest extends FunSuite {
  private val blarg = ColumnName("blarg")
  
  private object FlipDetectors {
    val alwaysFlipped: FlipDetector = new FlipDetector {
      override def isFlipped(variantId: String): Boolean = true
    }
    
    val neverFlipped: FlipDetector = new FlipDetector {
      override def isFlipped(variantId: String): Boolean = false
    }
  }
  
  private def assertCloseEnough(lhs: Double, rhs: Double, epsilon: Double = 0.000001): Unit = {
    assert(math.abs(lhs - rhs) < epsilon)
  }
  
  test("PassThru") {
    def doTest(
        f: ColumnName => ColumnDef,
        expectedDestColumn: ColumnName): Unit = {
  
      val sourceColumn = ColumnName("blarg")
      
      assert(f(sourceColumn) === ColumnDef(expectedDestColumn, sourceColumn))
    }
    
    import ColumnDefs.PassThru._
    
    doTest(beta(_), ColumnNames.beta)
    doTest(eaf(_), ColumnNames.eaf)
    doTest(maf(_), ColumnNames.maf)
    doTest(marker(_), ColumnNames.marker)
    doTest(n(_), ColumnNames.n)
    doTest(oddsRatio(_), ColumnNames.odds_ratio)
    doTest(pvalue(_), ColumnNames.pvalue)
    doTest(stderr(_), ColumnNames.stderr)
    doTest(zscore(_), ColumnNames.zscore)
  }
  
  test("just") {
    assert(ColumnDefs.just(blarg) === ColumnDef(blarg))
  }
  
  test("marker") {
    val foo = ColumnName("foo")
    val bar = ColumnName("bar")
    val baz = ColumnName("baz")
    val bip = ColumnName("bip")
    
    val markerDef = ColumnDefs.marker(chromColumn = foo, posColumn = bar, refColumn = baz, altColumn = bip)
    
    assert(markerDef.name === ColumnNames.marker)
    
    val row = Helpers.csvRow("foo" -> "42", "bar" -> "asdf", "baz" -> "123", "bip" -> "456")
    
    assert(markerDef.getValueFromSource.apply(row) == "42_asdf_123_456") 
    assert(markerDef.getValueFromSourceWhenFlipNeeded.get.apply(row) == "42_asdf_456_123")
  }
  
  test("pvalue") {
    val baz = ColumnName("baz")
    
    val pvalueDef = ColumnDefs.pvalue(baz)
    
    assert(pvalueDef.name === ColumnNames.pvalue)
    
    val row = Helpers.csvRow("foo" -> "42", "bar" -> "asdf", "baz" -> "1.23", "bip" -> "456")
    
    assert(pvalueDef.getValueFromSource.apply(row) == 1.23)
  }
  
  test("stderr") {
    val baz = ColumnName("baz")
    
    val stderrDef = ColumnDefs.stderr(baz)
    
    assert(stderrDef.name === ColumnNames.stderr)
    
    val row = Helpers.csvRow("foo" -> "42", "bar" -> "asdf", "baz" -> "1.23", "bip" -> "456")
    
    assert(stderrDef.getValueFromSource.apply(row) == 1.23)
  }
  
  test("zscore") {
    val baz = ColumnName("baz").asDouble
    
    val zscoreDef = ColumnDefs.zscore(baz)
    
    assert(zscoreDef.name === ColumnNames.zscore)
    
    val row = Helpers.csvRow("foo" -> "42", "bar" -> "asdf", "baz" -> "1.23", "bip" -> "456")
    
    assert(zscoreDef.getValueFromSource.apply(row) == 1.23)
  }
  
  test("zscoreFrom") {
    val baz = ColumnName("baz")
    val bip = ColumnName("bip")
    
    val zscoreDef = ColumnDefs.zscoreFrom(betaColumn = baz, stderrColumn = bip)
    
    assert(zscoreDef.name === ColumnNames.zscore)
    
    val row = Helpers.csvRow("foo" -> "42", "bar" -> "asdf", "baz" -> "1.2", "bip" -> "6")
    
    assertCloseEnough(zscoreDef.getValueFromSource.apply(row).asInstanceOf[Double], 0.2)
    assertCloseEnough(zscoreDef.getValueFromSourceWhenFlipNeeded.get.apply(row).asInstanceOf[Double], -0.2)
  }
  
  test("beta") {
    val baz = ColumnName("baz")
    
    val betaDef = ColumnDefs.beta(baz)
    
    assert(betaDef.name === ColumnNames.beta)
    
    val row = Helpers.csvRow("foo" -> "42", "bar" -> "asdf", "baz" -> "1.23", "bip" -> "456")
    
    assert(betaDef.getValueFromSource.apply(row) == 1.23)
    assert(betaDef.getValueFromSourceWhenFlipNeeded.get.apply(row) == -1.23)
  }

  test("eaf") {
    val baz = ColumnName("baz")
    
    val eafDef = ColumnDefs.eaf(baz)
    
    assert(eafDef.name === ColumnNames.eaf)
    
    val row = Helpers.csvRow("foo" -> "42", "bar" -> "asdf", "baz" -> "1.23", "bip" -> "456")
    
    assert(eafDef.getValueFromSource.apply(row) == 1.23)
    assert(eafDef.getValueFromSourceWhenFlipNeeded.get.apply(row) == (1.0 - 1.23))
  }
  
  test("oddsRatio") {
    val baz = ColumnName("baz")
    
    val oddsRatioDef = ColumnDefs.oddsRatio(baz)
    
    assert(oddsRatioDef.name === ColumnNames.odds_ratio)
    
    val row = Helpers.csvRow("foo" -> "42", "bar" -> "asdf", "baz" -> "1.23", "bip" -> "456")
    
    assert(oddsRatioDef.getValueFromSource.apply(row) == 1.23)
    assert(oddsRatioDef.getValueFromSourceWhenFlipNeeded.get.apply(row) == (1.0 / 1.23))
  }
}

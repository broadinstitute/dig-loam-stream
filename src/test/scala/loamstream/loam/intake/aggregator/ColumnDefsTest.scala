package loamstream.loam.intake.aggregator

import org.scalatest.FunSuite
import loamstream.loam.intake.NamedColumnDef
import loamstream.loam.intake.ColumnName
import loamstream.loam.intake.flip.FlipDetector
import loamstream.loam.intake.Helpers
import loamstream.loam.intake.ColumnExpr
import loamstream.loam.intake.flip.Disposition
import loamstream.loam.intake.Variant

/**
 * @author clint
 * Sep 28, 2020
 */
final class ColumnDefsTest extends FunSuite {
  private val blarg = ColumnName("blarg")
  
  private object FlipDetectors {
    val alwaysFlipped: FlipDetector = new FlipDetector {
      override def isFlipped(variantId: Variant): Disposition = Disposition.FlippedSameStrand
    }
    
    val neverFlipped: FlipDetector = new FlipDetector {
      override def isFlipped(variantId: Variant): Disposition = Disposition.NotFlippedSameStrand
    }
  }
  
  private def assertCloseEnough(lhs: Double, rhs: Double, epsilon: Double = 0.000001): Unit = {
    assert(math.abs(lhs - rhs) < epsilon)
  }
  
  test("PassThru") {
    def doTest(
        f: ColumnName => NamedColumnDef[_],
        expectedDestColumn: ColumnName, 
        isDouble: Boolean = false): Unit = {
  
      val sourceColumn = ColumnName("blarg")
      
      val passThruColumnDef = f(sourceColumn)
      
      assert(passThruColumnDef.name === expectedDestColumn)
      assert(passThruColumnDef.exprWhenFlipped.isEmpty)
      
      val row = Helpers.csvRow("foo" -> "42", "bar" -> "asdf", "blarg" -> "123", "bip" -> "456")
      
      val expected = if(isDouble) 123.0 else "123"
      
      assert(passThruColumnDef.expr.apply(row) == expected)
    }
    
    import AggregatorColumnDefs.PassThru._
    
    doTest(beta(_), ColumnNames.beta, isDouble = true)
    doTest(eaf(_), ColumnNames.eaf, isDouble = true)
    doTest(maf(_), ColumnNames.maf, isDouble = true)
    doTest(marker(_), ColumnNames.marker)
    doTest(n(_), ColumnNames.n, isDouble = true)
    doTest(oddsRatio(_), ColumnNames.odds_ratio, isDouble = true)
    doTest(pvalue(_), ColumnNames.pvalue, isDouble = true)
    doTest(stderr(_), ColumnNames.stderr, isDouble = true)
    doTest(zscore(_), ColumnNames.zscore, isDouble = true)
  }
  
  test("just") {
    assert(AggregatorColumnDefs.just(blarg) === NamedColumnDef(blarg))
  }
  
  test("marker") {
    val foo = ColumnName("foo")
    val bar = ColumnName("bar")
    val baz = ColumnName("baz")
    val bip = ColumnName("bip")
    
    val markerDef = AggregatorColumnDefs.marker(chromColumn = foo, posColumn = bar, refColumn = baz, altColumn = bip)
    
    assert(markerDef.name === ColumnNames.marker)
    
    val row = Helpers.csvRow("foo" -> "42", "bar" -> "asdf", "baz" -> "123", "bip" -> "456")
    
    assert(markerDef.expr.apply(row) == "42_asdf_123_456") 
    assert(markerDef.exprWhenFlipped.get.apply(row) == "42_asdf_456_123")
  }
  
  test("pvalue") {
    val baz = ColumnName("baz")
    
    val pvalueDef = AggregatorColumnDefs.pvalue(baz)
    
    assert(pvalueDef.name === ColumnNames.pvalue)
    
    val row = Helpers.csvRow("foo" -> "42", "bar" -> "asdf", "baz" -> "1.23", "bip" -> "456")
    
    assert(pvalueDef.expr.apply(row) == 1.23)
  }
  
  test("stderr") {
    val baz = ColumnName("baz")
    
    val stderrDef = AggregatorColumnDefs.stderr(baz)
    
    assert(stderrDef.name === ColumnNames.stderr)
    
    val row = Helpers.csvRow("foo" -> "42", "bar" -> "asdf", "baz" -> "1.23", "bip" -> "456")
    
    assert(stderrDef.expr.apply(row) == 1.23)
  }
  
  test("zscore") {
    val baz = ColumnName("baz").asDouble
    
    val zscoreDef = AggregatorColumnDefs.zscore(baz)
    
    assert(zscoreDef.name === ColumnNames.zscore)
    
    val row = Helpers.csvRow("foo" -> "42", "bar" -> "asdf", "baz" -> "1.23", "bip" -> "456")
    
    assert(zscoreDef.expr.apply(row) == 1.23)
  }
  
  test("zscoreFrom") {
    val baz = ColumnName("baz")
    val bip = ColumnName("bip")
    
    val zscoreDef = AggregatorColumnDefs.zscoreFrom(betaColumn = baz, stderrColumn = bip)
    
    assert(zscoreDef.name === ColumnNames.zscore)
    
    val row = Helpers.csvRow("foo" -> "42", "bar" -> "asdf", "baz" -> "1.2", "bip" -> "6")
    
    assertCloseEnough(zscoreDef.expr.apply(row).asInstanceOf[Double], 0.2)
    assertCloseEnough(zscoreDef.exprWhenFlipped.get.apply(row).asInstanceOf[Double], -0.2)
  }
  
  test("beta") {
    val baz = ColumnName("baz")
    
    val betaDef = AggregatorColumnDefs.beta(baz)
    
    assert(betaDef.name === ColumnNames.beta)
    
    val row = Helpers.csvRow("foo" -> "42", "bar" -> "asdf", "baz" -> "1.23", "bip" -> "456")
    
    assert(betaDef.expr.apply(row) == 1.23)
    assert(betaDef.exprWhenFlipped.get.apply(row) == -1.23)
  }

  test("eaf") {
    val baz = ColumnName("baz")
    
    val eafDef = AggregatorColumnDefs.eaf(baz)
    
    assert(eafDef.name === ColumnNames.eaf)
    
    val row = Helpers.csvRow("foo" -> "42", "bar" -> "asdf", "baz" -> "1.23", "bip" -> "456")
    
    assert(eafDef.expr.apply(row) == 1.23)
    assert(eafDef.exprWhenFlipped.get.apply(row) == (1.0 - 1.23))
  }
  
  test("oddsRatio") {
    val baz = ColumnName("baz")
    
    val oddsRatioDef = AggregatorColumnDefs.oddsRatio(baz)
    
    assert(oddsRatioDef.name === ColumnNames.odds_ratio)
    
    val row = Helpers.csvRow("foo" -> "42", "bar" -> "asdf", "baz" -> "1.23", "bip" -> "456")
    
    assert(oddsRatioDef.expr.apply(row) == 1.23)
    assert(oddsRatioDef.exprWhenFlipped.get.apply(row) == (1.0 / 1.23))
  }
}

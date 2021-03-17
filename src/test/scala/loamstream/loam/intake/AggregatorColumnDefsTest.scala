package loamstream.loam.intake

import org.scalatest.FunSuite
import loamstream.loam.intake.flip.FlipDetector
import loamstream.loam.intake.flip.Disposition
import org.scalactic.source.Position.apply

/**
 * @author clint
 * Sep 28, 2020
 */
final class AggregatorColumnDefsTest extends FunSuite {
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
        f: ColumnName => HandlesFlipsColumnDef[_],
        expectedDestColumn: ColumnName, 
        isDouble: Boolean = false): Unit = {
  
      val sourceColumn = ColumnName("blarg")
      
      val passThruColumnDef = f(sourceColumn)
      
      assert(passThruColumnDef.exprWhenFlipped.isEmpty)
      
      val row = Helpers.csvRow("foo" -> "42", "bar" -> "asdf", "blarg" -> "123", "bip" -> "456")
      
      val expected = if(isDouble) 123.0 else "123"
      
      assert(passThruColumnDef.expr.apply(row) == expected)
    }
    
    import AggregatorColumnDefs.PassThru._
    
    doTest(beta(_), AggregatorColumnNames.beta, isDouble = true)
    doTest(eaf(_), AggregatorColumnNames.eaf, isDouble = true)
    doTest(maf(_), AggregatorColumnNames.maf, isDouble = true)
    doTest(marker(_), AggregatorColumnNames.marker)
    doTest(n(_), AggregatorColumnNames.n, isDouble = true)
    doTest(oddsRatio(_), AggregatorColumnNames.odds_ratio, isDouble = true)
    doTest(pvalue(_), AggregatorColumnNames.pvalue, isDouble = true)
    doTest(stderr(_), AggregatorColumnNames.stderr, isDouble = true)
    doTest(zscore(_), AggregatorColumnNames.zscore, isDouble = true)
  }
  
  test("just") {
    assert(AggregatorColumnDefs.just(blarg) === AnonColumnDef(blarg))
  }
  
  test("marker") {
    val foo = ColumnName("foo")
    val bar = ColumnName("bar")
    val baz = ColumnName("baz")
    val bip = ColumnName("bip")
    
    val markerDef = AggregatorColumnDefs.marker(chromColumn = foo, posColumn = bar, refColumn = baz, altColumn = bip)
    
    assert(markerDef.name === AggregatorColumnNames.marker)
    
    val row = Helpers.csvRow("foo" -> "17", "bar" -> "123456", "baz" -> "A", "bip" -> "T")
    
    assert(markerDef(row) == Variant.from("17_123456_A_T")) 
  }
  
  test("marker - force alphabetic chrom names") {
    val foo = ColumnName("foo")
    val bar = ColumnName("bar")
    val baz = ColumnName("baz")
    val bip = ColumnName("bip")
    
    def doTest(ensureAlphabeticChromNames: Boolean): Unit = {
      val markerDef = AggregatorColumnDefs.marker(
          chromColumn = foo, 
          posColumn = bar, 
          refColumn = baz, 
          altColumn = bip,
          forceAlphabeticChromNames = ensureAlphabeticChromNames)
      
      assert(markerDef.name === AggregatorColumnNames.marker)
      
      val row = Helpers.csvRow("foo" -> "26", "bar" -> "123456", "baz" -> "A", "bip" -> "T")
      
      val expectedString = if(ensureAlphabeticChromNames) "MT_123456_A_T" else "26_123456_A_T" 
      
      assert(markerDef(row) == Variant.from(expectedString))
    }
    
    doTest(true)
    doTest(false)
  }
  
  test("marker - upper-case alleles") {
    val foo = ColumnName("foo")
    val bar = ColumnName("bar")
    val baz = ColumnName("baz")
    val bip = ColumnName("bip")
    
    def doTest(uppercaseAlleles: Boolean): Unit = {
      val markerDef = AggregatorColumnDefs.marker(
          chromColumn = foo, 
          posColumn = bar, 
          refColumn = baz, 
          altColumn = bip,
          uppercaseAlleles = uppercaseAlleles)
      
      assert(markerDef.name === AggregatorColumnNames.marker)
      
      val row = Helpers.csvRow("foo" -> "17", "bar" -> "123456", "baz" -> "a", "bip" -> "t")
      
      val expectedString = if(uppercaseAlleles) "17_123456_A_T" else "17_123456_a_t" 
      
      assert(markerDef(row) == Variant.from(expectedString))
    }
    
    doTest(true)
    doTest(false)
  }
  
  test("pvalue") {
    val baz = ColumnName("baz")
    
    val pvalueDef = AggregatorColumnDefs.pvalue(baz)
    
    val row = Helpers.csvRow("foo" -> "42", "bar" -> "asdf", "baz" -> "1.23", "bip" -> "456")
    
    assert(pvalueDef.expr.apply(row) == 1.23)
  }
  
  test("stderr") {
    val baz = ColumnName("baz")
    
    val stderrDef = AggregatorColumnDefs.stderr(baz)
    
    val row = Helpers.csvRow("foo" -> "42", "bar" -> "asdf", "baz" -> "1.23", "bip" -> "456")
    
    assert(stderrDef.expr.apply(row) == 1.23)
  }
  
  test("zscore") {
    val baz = ColumnName("baz").asDouble
    
    val zscoreDef = AggregatorColumnDefs.zscore(baz)
    
    val row = Helpers.csvRow("foo" -> "42", "bar" -> "asdf", "baz" -> "1.23", "bip" -> "456")
    
    assert(zscoreDef.expr.apply(row) == 1.23)
  }
  
  test("zscoreFrom") {
    val baz = ColumnName("baz")
    val bip = ColumnName("bip")
    
    val zscoreDef = AggregatorColumnDefs.zscoreFrom(betaColumn = baz, stderrColumn = bip)
    
    val row = Helpers.csvRow("foo" -> "42", "bar" -> "asdf", "baz" -> "1.2", "bip" -> "6")
    
    assertCloseEnough(zscoreDef.expr.apply(row).asInstanceOf[Double], 0.2)
    assertCloseEnough(zscoreDef.exprWhenFlipped.get.apply(row).asInstanceOf[Double], -0.2)
  }
  
  test("beta") {
    val baz = ColumnName("baz")
    
    val betaDef = AggregatorColumnDefs.beta(baz)
    
    val row = Helpers.csvRow("foo" -> "42", "bar" -> "asdf", "baz" -> "1.23", "bip" -> "456")
    
    assert(betaDef.expr.apply(row) == 1.23)
    assert(betaDef.exprWhenFlipped.get.apply(row) == -1.23)
  }

  test("eaf") {
    val baz = ColumnName("baz")
    
    val eafDef = AggregatorColumnDefs.eaf(baz)
    
    val row = Helpers.csvRow("foo" -> "42", "bar" -> "asdf", "baz" -> "1.23", "bip" -> "456")
    
    assert(eafDef.expr.apply(row) == 1.23)
    assert(eafDef.exprWhenFlipped.get.apply(row) == (1.0 - 1.23))
  }
  
  test("oddsRatio") {
    val baz = ColumnName("baz")
    
    val oddsRatioDef = AggregatorColumnDefs.oddsRatio(baz)
    
    val row = Helpers.csvRow("foo" -> "42", "bar" -> "asdf", "baz" -> "1.23", "bip" -> "456")
    
    assert(oddsRatioDef.expr.apply(row) == 1.23)
    assert(oddsRatioDef.exprWhenFlipped.get.apply(row) == (1.0 / 1.23))
  }
}

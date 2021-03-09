package loamstream.loam.intake

import org.scalatest.FunSuite
import loamstream.TestHelpers
import loamstream.loam.LoamSyntax
import java.nio.file.Files

/**
 * @author clint
 * Nov 6, 2020
 */
final class RowTransformsTest extends FunSuite {
  private object Transforms extends IntakeSyntax with RowTransforms
  
  private val v0 = Variant("1_12345_a_t")
  private val v1 = Variant("2_34567_T_c")
  private val v2 = Variant("3_45678_g_T")
  
  import Helpers.withLogStore
  import Helpers.linesFrom
  import Helpers.Implicits.LogFileOps
  
  private val metadata = AggregatorMetadata(
    dataset = "asdasdasd",
    phenotype = "akjdslfhsdf",
    ancestry = Ancestry.AA,
    tech = TechType.ExChip,
    quantitative = None)
    
  private def makeRow(
      marker: Variant, 
      pvalue: Double, 
      eaf: Option[Double] = None, 
      maf: Option[Double] = None): AggregatorVariantRow = {
    
    AggregatorVariantRow(
      marker = marker,
      pvalue = pvalue,
      dataset = metadata.dataset,
      phenotype = metadata.phenotype,
      ancestry = metadata.ancestry,
      eaf = eaf,
      maf = maf)
  }
  
  test("upperCaseAlleles") {
    val rows = Seq(
        makeRow(marker = v0, pvalue = 42.0),
        makeRow(marker = v1, pvalue = 42.0),
        makeRow(marker = v2, pvalue = 42.0))
        
    val actual = rows.map(Transforms.DataRowTransforms.upperCaseAlleles)
    
    val expected = Seq(
        makeRow(marker = v0.toUpperCase, pvalue = 42.0),
        makeRow(marker = v1.toUpperCase, pvalue = 42.0),
        makeRow(marker = v2.toUpperCase, pvalue = 42.0))
        
    assert(actual === expected)
  }
  
  test("clampPvalues") {
    val rows = Seq(
        makeRow(marker = v0, pvalue = 0.0),
        makeRow(marker = v1, pvalue = 42.0),
        makeRow(marker = v2, pvalue = 0.0))
        
    val expected = Seq(
        makeRow(marker = v0, pvalue = Double.MinPositiveValue),
        makeRow(marker = v1, pvalue = 42.0),
        makeRow(marker = v2, pvalue = Double.MinPositiveValue))
        
    withLogStore { logStore =>
      val transform = Transforms.DataRowTransforms.clampPValues(logStore, append = true)
      
      val actual = rows.map(transform)
      
      transform.close()
      
      assert(actual === expected)
      
      val loggedLines = linesFrom(logStore.path)
      
      assert(loggedLines.containsOnce(v0.underscoreDelimited))
      assert(loggedLines.containsOnce(v2.underscoreDelimited))
      assert(loggedLines.size == 2)
    }
  }
}

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
  
  test("upperCaseAlleles") {
    val rows = Seq(
        DataRow(marker = v0, pvalue = 42.0),
        DataRow(marker = v1, pvalue = 42.0),
        DataRow(marker = v2, pvalue = 42.0))
        
    val actual = rows.map(Transforms.DataRowTransforms.upperCaseAlleles)
    
    val expected = Seq(
        DataRow(marker = v0.toUpperCase, pvalue = 42.0),
        DataRow(marker = v1.toUpperCase, pvalue = 42.0),
        DataRow(marker = v2.toUpperCase, pvalue = 42.0))
        
    assert(actual === expected)
  }
  
  test("clampPvalues") {
    val rows = Seq(
        DataRow(marker = v0, pvalue = 0.0),
        DataRow(marker = v1, pvalue = 42.0),
        DataRow(marker = v2, pvalue = 0.0))
        
    val expected = Seq(
        DataRow(marker = v0, pvalue = Double.MinPositiveValue),
        DataRow(marker = v1, pvalue = 42.0),
        DataRow(marker = v2, pvalue = Double.MinPositiveValue))
        
    TestHelpers.withWorkDir(getClass.getSimpleName) { workDir =>
      TestHelpers.withScriptContext { implicit context =>
        import LoamSyntax._
        
        val logFile = workDir.resolve("foo.log")
        
        val logStore = store(logFile)
        
        val actual = rows.map(Transforms.DataRowTransforms.clampPValues(logStore, append = true))
        
        assert(actual === expected)
        
        val loggedLines = {
          import scala.collection.JavaConverters._
          
          Files.readAllLines(logFile).asScala.toList
        }
        
        assert(loggedLines.count(_.contains(v0.underscoreDelimited)) === 1)
        assert(loggedLines.count(_.contains(v2.underscoreDelimited)) === 1)
        assert(loggedLines.size == 2)
      }
    }
  }
}

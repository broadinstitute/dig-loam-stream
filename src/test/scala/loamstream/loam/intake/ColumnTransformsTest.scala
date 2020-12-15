package loamstream.loam.intake

import org.scalatest.FunSuite

/**
 * @author clint
 * Oct 29, 2020
 */
final class ColumnTransformsTest extends FunSuite {
  test("ensureAlphabeticChromNames") {
    val a = "A"
    val b = "B"
    
    val inputChromNames = (1 to 26).map(_.toString) ++ Seq("M", "m")
    
    val rows = Helpers.csvRows(
        Seq("A", "B"),
        inputChromNames.map(c => Seq(c.toString, "foo")): _*)
        
    val expr = ColumnTransforms.ensureAlphabeticChromNames(ColumnName("A"))
    
    val actual = rows.map(expr)
    
    val expected = (1 to 22).map(_.toString) ++ Seq("X", "Y", "XY", "MT", "MT", "MT")
    
    assert(actual === expected)
  }
}

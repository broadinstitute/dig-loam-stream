package loamstream.loam.intake.dga

import org.scalatest.FunSuite
import org.json4s._

/**
 * @author clint
 * Feb 16, 2021
 */
final class JsonTest extends FunSuite {
  test("toJValue") {
    import Json.toJValue
    
    assert(toJValue(Seq("x", "y")) === JArray(List(JString("x"), JString("y"))))
    
    assert(toJValue(Option(Seq("x", "y"))) === JArray(List(JString("x"), JString("y"))))
    
    assert(toJValue(None) === JNull)
  }
}

package loamstream.loam

import org.scalatest.FunSuite

/**
 * @author clint
 * May 29, 2020
 */
final class ScriptTypeTest extends FunSuite {
  test("name/suffix") {
    import ScriptType.{Loam, Scala}
    
    assert(Loam.name === "loam")
    assert(Loam.suffix === ".loam")
    
    assert(Scala.name === "scala")
    assert(Scala.suffix === ".scala")
  }
}

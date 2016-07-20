package loamstream.util

import org.scalatest.FunSuite

/**
 * @author clint
 * date: Jul 19, 2016
 */
final class SourceUtilsTest extends FunSuite {
  
  import SourceUtils._

  test("fullTypeName") {
    assert(fullTypeName[String] == classOf[String].getName)
    
    assert(fullTypeName[SourceUtilsTest.Foo] == s"${classOf[SourceUtilsTest].getName}.Foo")
    
    assert(fullTypeName[SourceUtilsTest.Bar] == s"${classOf[SourceUtilsTest].getName}.Bar")
    
    assert(fullTypeName[SourceUtilsTest.Baz] == s"${classOf[SourceUtilsTest].getName}.Baz")
  }
  
  test("shortTypeName") {
    assert(shortTypeName[String] == "String")
    
    assert(shortTypeName[SourceUtilsTest.Foo] == "Foo")
    
    assert(shortTypeName[SourceUtilsTest.Bar] == "Bar")
    
    assert(shortTypeName[SourceUtilsTest.Baz] == "Baz")
  }
}

object SourceUtilsTest {
  class Foo
  
  trait Bar
  
  case class Baz()
}
package loamstream.util

import org.scalatest.FunSuite

/**
 * @author clint
 * Sep 21, 2017
 */
final class ClassesTest extends FunSuite {
  test("simpleNameOf") {
    import Classes.simpleNameOf
    
    assert(simpleNameOf[String] === "String")
    assert(simpleNameOf[ClassesTest.Foo] === "Foo")
    
    assert(simpleNameOf("") === "String")
    assert(simpleNameOf(new ClassesTest.Foo) === "Foo")
  }
}

object ClassesTest {
  private final class Foo
}

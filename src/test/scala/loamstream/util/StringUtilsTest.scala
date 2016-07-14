package loamstream.util

import org.scalatest.FunSuite

/**
  * @author clint
  *         date: Apr 14, 2016
  */
final class StringUtilsTest extends FunSuite {
  test("isWhitespace()") {
    import StringUtils.isWhitespace

    assert(isWhitespace("") === true)
    assert(isWhitespace(" ") === true)
    assert(isWhitespace("   ") === true)
    assert(isWhitespace("\t") === true)
    assert(isWhitespace("\r\n") === true)
    assert(isWhitespace(" \t") === true)
    assert(isWhitespace("\r\n\t") === true)

    assert(isWhitespace("x") === false)
    assert(isWhitespace("x ") === false)
    assert(isWhitespace("x   ") === false)
    assert(isWhitespace("x\t") === false)
    assert(isWhitespace("x\r\n") === false)
    assert(isWhitespace("x \t") === false)
    assert(isWhitespace("x\r\n\t") === false)

    assert(isWhitespace(" x") === false)
    assert(isWhitespace("   x") === false)
    assert(isWhitespace("\tx") === false)
    assert(isWhitespace("\r\nx") === false)
    assert(isWhitespace(" \tx") === false)
    assert(isWhitespace("\r\n\tx") === false)
  }

  test("isNotWhitespace()") {
    import StringUtils.isNotWhitespace

    assert(isNotWhitespace("") === false)
    assert(isNotWhitespace(" ") === false)
    assert(isNotWhitespace("   ") === false)
    assert(isNotWhitespace("\t") === false)
    assert(isNotWhitespace("\r\n") === false)
    assert(isNotWhitespace(" \t") === false)
    assert(isNotWhitespace("\r\n\t") === false)

    assert(isNotWhitespace("x") === true)
    assert(isNotWhitespace("x ") === true)
    assert(isNotWhitespace("x   ") === true)
    assert(isNotWhitespace("x\t") === true)
    assert(isNotWhitespace("x\r\n") === true)
    assert(isNotWhitespace("x \t") === true)
    assert(isNotWhitespace("x\r\n\t") === true)

    assert(isNotWhitespace(" x") === true)
    assert(isNotWhitespace("   x") === true)
    assert(isNotWhitespace("\tx") === true)
    assert(isNotWhitespace("\r\nx") === true)
    assert(isNotWhitespace(" \tx") === true)
    assert(isNotWhitespace("\r\n\tx") === true)
  }

  test("IsLong") {
    import StringUtils.IsLong

    val IsLong(x) = "123"

    assert(x === 123L)

    assert(IsLong.unapply("") === None)
    assert(IsLong.unapply("  ") === None)
    assert(IsLong.unapply("asdf") === None)
    assert(IsLong.unapply(" 42 ") === None)
    assert(IsLong.unapply("1.23") === None)
  }
  
  test("unwrapLines") {
    val text = "Hello\nWorld!\rHow\r\nare\n\ryou?"
    assert(StringUtils.unwrapLines(text) === "Hello World! How are you?")
  }
  
  test("soMany(count, singular, plural)") {
    import StringUtils.soMany
    
    assert(soMany(0, "foo", "Foos") == "no Foos")
    assert(soMany(1, "foo", "Foos") == "one foo")
    
    assert(soMany(2, "foo", "Foos") == "two Foos")
    assert(soMany(3, "foo", "Foos") == "three Foos")
    assert(soMany(4, "foo", "Foos") == "four Foos")
    assert(soMany(5, "foo", "Foos") == "five Foos")
    assert(soMany(6, "foo", "Foos") == "six Foos")
    assert(soMany(7, "foo", "Foos") == "seven Foos")
    assert(soMany(8, "foo", "Foos") == "eight Foos")
    assert(soMany(9, "foo", "Foos") == "nine Foos")
    assert(soMany(10, "foo", "Foos") == "ten Foos")
    assert(soMany(11, "foo", "Foos") == "eleven Foos")
    assert(soMany(12, "foo", "Foos") == "twelve Foos")
  
    assert(soMany(13, "foo", "Foos") == "13 Foos")
    assert(soMany(42, "foo", "Foos") == "42 Foos")
  }
  
  test("soMany(count, singular)") {
    import StringUtils.soMany
    
    assert(soMany(0, "Foo") == "no Foos")
    assert(soMany(1, "Foo") == "one Foo")
    
    assert(soMany(2, "Foo") == "two Foos")
    assert(soMany(3, "Foo") == "three Foos")
    assert(soMany(4, "Foo") == "four Foos")
    assert(soMany(5, "Foo") == "five Foos")
    assert(soMany(6, "Foo") == "six Foos")
    assert(soMany(7, "Foo") == "seven Foos")
    assert(soMany(8, "Foo") == "eight Foos")
    assert(soMany(9, "Foo") == "nine Foos")
    assert(soMany(10, "Foo") == "ten Foos")
    assert(soMany(11, "Foo") == "eleven Foos")
    assert(soMany(12, "Foo") == "twelve Foos")
    
    assert(soMany(13, "Foo") == "13 Foos")
    assert(soMany(42, "Foo") == "42 Foos")
  }
}
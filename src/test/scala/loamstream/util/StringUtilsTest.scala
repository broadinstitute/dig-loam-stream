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
}
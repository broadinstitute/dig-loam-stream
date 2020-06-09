package loamstream.util

import org.scalatest.FunSuite

/**
 * @author clint
 * Jun 5, 2020
 */
final class Base64Test extends FunSuite {
  import Base64.{encode, decode}

  private val someBytes = "FOO lalala".getBytes

  test("encode") {
    assert(encode(someBytes) === "Rk9PIGxhbGFsYQ==")

	assert(encode(Array.empty) === "")
  }

  test("decode") {
    assert(decode("Rk9PIGxhbGFsYQ==") === someBytes)

    assert(decode("") === Array.empty[Byte])
  }
}

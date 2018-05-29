package loamstream.db.slick

import org.scalatest.FunSuite

/**
  * @author clint
  *         date: Aug 10, 2016
  */
final class HelpersTest extends FunSuite {
  test("timestampFromLong") {
    val millis = 123456

    val timestamp = Helpers.timestampFromLong(millis)

    assert(timestamp.toInstant.toEpochMilli === millis)
  }
}

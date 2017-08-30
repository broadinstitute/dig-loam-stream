package loamstream.util

import org.scalatest.FunSuite

/**
 * @author kyuksel
 *         date: 8/30/17
 */
final class CanBeClosedTest extends FunSuite {
  test("enclosed() closes things properly") {
    final class Foo {
      var isClosed = false

      def close(): Unit = isClosed = true
    }

    implicit object FoosCanBeClosed extends CanBeClosed[Foo] {
      override def close(f: Foo): Unit = f.close()
    }

    val foo = new Foo

    assert(foo.isClosed === false)

    val result = CanBeClosed.enclosed(foo) { foo =>
      42
    }

    assert(result === 42)
    assert(foo.isClosed === true)
  }
}

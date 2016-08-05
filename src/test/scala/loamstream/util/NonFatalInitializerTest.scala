package loamstream.util

import org.scalatest.FunSuite

/** Tests for NonFatalInitializer */
class NonFatalInitializerTest extends FunSuite {
  val nonFatalError = new AssertionError()
  val exInInitErrorWithNonFatalError = new ExceptionInInitializerError(nonFatalError)
  val fatalError = new OutOfMemoryError()
  val exInInitErrorWithFatalError = new ExceptionInInitializerError(fatalError)

  val nonFatal = "nonFatal"
  val initializerNonFatal = "initializerNonFatal"
  val other = "other"

  def applyFold(throwable: Throwable): String =
    NonFatalInitializer.fold(throwable, nonFatal, initializerNonFatal, other)

  test("NonFatalInitializer.fold") {
    assert(applyFold(nonFatalError) === nonFatal)
    assert(applyFold(exInInitErrorWithNonFatalError) === initializerNonFatal)
    assert(applyFold(fatalError) === other)
    assert(applyFold(exInInitErrorWithFatalError) === other)
  }

  test("NonFatalInitializer.apply") {
    assert(NonFatalInitializer(nonFatalError))
    assert(NonFatalInitializer(exInInitErrorWithNonFatalError))
    assert(!NonFatalInitializer(fatalError))
    assert(!NonFatalInitializer(exInInitErrorWithFatalError))
  }

  test("NonFatalInitializer.unapply") {
    assert(NonFatalInitializer.unapply(nonFatalError) === Some(nonFatalError))
    assert(NonFatalInitializer.unapply(exInInitErrorWithNonFatalError) === Some(nonFatalError))
    assert(NonFatalInitializer.unapply(fatalError) === None)
    assert(NonFatalInitializer.unapply(exInInitErrorWithFatalError) === None)
  }

}

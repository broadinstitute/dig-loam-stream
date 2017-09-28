package loamstream.compiler

import org.scalatest.FunSuite

/** Tests for NonFatalInitializer */
final class ReportableCompilationErrorTest extends FunSuite {
  test("NonFatalInitializer.unapply") {
    val nonFatalError = new AssertionError
    val exInInitErrorWithNonFatalError = new ExceptionInInitializerError(nonFatalError)
    val fatalError = new OutOfMemoryError
    val exInInitErrorWithFatalError = new ExceptionInInitializerError(fatalError)
    
    assert(ReportableCompilationError.unapply(nonFatalError) === Some(nonFatalError))
    assert(ReportableCompilationError.unapply(exInInitErrorWithNonFatalError) === Some(nonFatalError))
    assert(ReportableCompilationError.unapply(fatalError) === None)
    assert(ReportableCompilationError.unapply(exInInitErrorWithFatalError) === None)
  }
}

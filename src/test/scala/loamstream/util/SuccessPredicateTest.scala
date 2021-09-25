package loamstream.util

import org.scalatest.FunSuite

/**
  * @author clint
  * @date Aug 17, 2021
  *
  */
final class SuccessPredicateTest extends FunSuite {
  private def makeResult(ec: Int, stdout: Seq[String] = Nil, stderr: Seq[String] = Nil): RunResults = {
    RunResults.Completed("foo", ec, stdout = stdout, stderr = stderr)
  }

  private def makeCouldNotStart: RunResults.CouldNotStart = RunResults.CouldNotStart("foo", new Exception)

  test("zeroIsSuccess") {
    import RunResults.SuccessPredicate.zeroIsSuccess

    assert(zeroIsSuccess(makeCouldNotStart) === false)
    assert(zeroIsSuccess(makeResult(1)) === false)
    assert(zeroIsSuccess(makeResult(42)) === false)

    assert(zeroIsSuccess(makeResult(0)) === true)
  }

  test("countsAsSuccess") {
    val p = RunResults.SuccessPredicate.ByExitCode.countsAsSuccess(1, 42)

    assert(p(makeCouldNotStart) === false)

    assert(p(makeResult(1)) === true)
    assert(p(makeResult(42)) === true)

    assert(p(makeResult(0)) === false)
  }

  test("||") {
    val hasEvenEc = RunResults.SuccessPredicate { 
      case RunResults.Completed(_, ec, _, _) => ec % 2 == 0
      case _ => false
    }

    val hasStdErr = RunResults.SuccessPredicate { 
      case RunResults.Completed(_, _, _, stderr) => stderr.nonEmpty
      case _ => false
    }

    val evenOrStderr = hasEvenEc || hasStdErr

    assert(evenOrStderr(makeResult(2)) === true)
    assert(evenOrStderr(makeResult(1)) === false)
    assert(evenOrStderr(makeResult(1, stderr = Seq("asdf"))) === true)
    assert(evenOrStderr(makeResult(2, stderr = Seq("asdf"))) === true)
  }

  test("&&") {
    val hasEvenEc = RunResults.SuccessPredicate { 
      case RunResults.Completed(_, ec, _, _) => ec % 2 == 0
      case _ => false
    }

    val hasStdErr = RunResults.SuccessPredicate { 
      case RunResults.Completed(_, _, _, stderr) => stderr.nonEmpty
      case _ => false
    }

    val evenAndStderr = hasEvenEc && hasStdErr

    assert(evenAndStderr(makeResult(2)) === false)
    assert(evenAndStderr(makeResult(1)) === false)
    assert(evenAndStderr(makeResult(1, stderr = Seq("asdf"))) === false)
    assert(evenAndStderr(makeResult(2, stderr = Seq("asdf"))) === true)
  }
}

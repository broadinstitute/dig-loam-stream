package loamstream.util

import java.util.concurrent.TimeUnit

import org.scalatest.FunSuite

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Failure

/** Tests of EvalLaterBox */
class EvalLaterBoxTest extends FunSuite {
  var counter: Int = 0
  var counterCompare: Int = 0

  val futureAwaitTimeInMillis = 100L
  val futureAwaitTime = Duration(futureAwaitTimeInMillis, TimeUnit.MILLISECONDS)

  def assertCounter(value: Int): Unit = {
    counterCompare += 1
    assert(value === counterCompare)
    assert(counter === counterCompare)
  }

  test("Expression evaluates successfully, with side effect.") {
    val box = EvalLaterBox {
      counter += 1
      counter
    }
    assert(counter === 0)
    assertCounter(box.eval)
    assertCounter(box.evalTry.get)
    assertCounter(box.evalShot.get)
    assertCounter(Await.result(box.evalFuture, futureAwaitTime))
  }

  class EvalLaterTestExpectedException(msg: String) extends Exception(msg)

  val myException = new EvalLaterTestExpectedException("Oops, I threw it again!")
  test("Expression throws exception") {
    val box = EvalLaterBox[Int] {
      throw myException
    }
    assertThrows[EvalLaterTestExpectedException](box.eval)
    assert(box.evalTry.asInstanceOf[Failure[Int]].exception === myException)
    assert(box.evalShot.asInstanceOf[Miss].snag.asInstanceOf[SnagThrowable].throwable === myException)
    assert(Await.ready(box.evalFuture, futureAwaitTime).value.get.asInstanceOf[Failure[Int]].exception === myException)
  }
}

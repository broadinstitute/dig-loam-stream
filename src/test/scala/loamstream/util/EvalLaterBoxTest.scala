package loamstream.util

import java.util.concurrent.TimeUnit

import org.scalatest.FunSuite

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Failure
import scala.util.control.NonFatal
import scala.reflect.runtime.universe.typeOf

/** Tests of EvalLaterBox */
final class EvalLaterBoxTest extends FunSuite {
  test("Type is stored") {
    val intBox = EvalLaterBox { 1 }
    assert(intBox.tpe =:= typeOf[Int] )
    val stringBox = EvalLaterBox { "Hello World!"}
    assert(stringBox.tpe =:= typeOf[String])
  }

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

  val myException = new Exception("Oops, I threw it again!")

  test("Expression throws exception") {
    val box = EvalLaterBox[Int] {
      throw myException
    }
    assert((try {
      box.eval
    } catch {
      case NonFatal(throwable) => throwable
    }) === myException)
    assert(box.evalTry.asInstanceOf[Failure[Int]].exception === myException)
    assert(box.evalShot.asInstanceOf[Miss].snag.asInstanceOf[SnagThrowable].throwable === myException)
    assert(Await.ready(box.evalFuture, futureAwaitTime).value.get.asInstanceOf[Failure[Int]].exception === myException)
  }

  test("Prepending, appending and wrapping") {
    var seq: Seq[Int] = Seq.empty
    val box = EvalLaterBox(seq :+= 2).prepend(seq :+= 1).append(seq :+= 3).wrap(seq :+= 0, seq :+= 4)
    assert(seq.isEmpty)
    box.eval
    assert(seq === Seq(0, 1, 2, 3, 4)) // scalastyle:ignore magic.number
  }
}

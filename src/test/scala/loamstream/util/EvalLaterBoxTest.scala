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

  val futureAwaitTimeInMillis = 100L
  val futureAwaitTime = Duration(futureAwaitTimeInMillis, TimeUnit.MILLISECONDS)

  test("Expression evaluates successfully, with side effect.") {
    @volatile var counter: Int = 0
    
    val box = EvalLaterBox {
      counter += 1
      counter
    }
    assert(counter === 0)
    
    assert(box.eval === 1)
    assert(counter === 1)

    assert(box.evalTry.get === 2)
    assert(counter === 2)
    
    assert(box.evalShot.get === 3)
    assert(counter === 3)
    
    assert(Await.result(box.evalFuture, futureAwaitTime) === 4)
    assert(counter === 4)
  }

  val myException = new Exception("Oops, I threw it again!")

  test("Expression throws exception") {
    val box = EvalLaterBox[Int] {
      throw myException
    }
    
    def assertThrewRightException(block: => Any): Unit = {
      val caught = intercept[Exception] { block }
      
      assert(caught === myException)
    }
    
    assertThrewRightException { box.eval }

    assert(box.evalTry === Failure(myException))
    
    assert(box.evalShot === Miss(Snag(myException)))
    
    assertThrewRightException { Await.result(box.evalFuture, futureAwaitTime) }
  }
}

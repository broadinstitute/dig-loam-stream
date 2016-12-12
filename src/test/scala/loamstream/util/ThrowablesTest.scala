package loamstream.util

import org.scalatest.FunSuite
import scala.Vector

/**
 * @author clint
 * Dec 7, 2016
 */
final class ThrowablesTest extends FunSuite {

  type ParamTuple = (Loggable.Level.Value, String, Throwable)
  
  private final class MockLogContext extends LogContext {
    
    val params: ValueBox[Seq[ParamTuple]] = ValueBox(Vector.empty)
    
    override def log(level: Loggable.Level.Value, s: => String, e: Throwable): Unit = {
      params.mutate(_ :+ (level, s, e))
    }
  }
  
  import Throwables.quietly
  
  test("quietly - defaults, nothing thrown") {
    implicit val logContext = new MockLogContext
    
    var x = 42
    
    assert(x === 42)
    
    quietly("foo")(x += 1)
    
    assert(x === 43)
    
    assert(logContext.params() === Vector.empty)
  }
  
  test("quietly - defaults, something thrown") {
    implicit val logContext = new MockLogContext
    
    var x = 42
    
    assert(x === 42)
    
    val e = new Exception
    
    quietly("foo")(throw e)
    
    assert(x === 42)
    
    assert(logContext.params() === Vector((Loggable.Level.error, "foo", e)))
  }
  
  test("quietly - nothing thrown") {
    def doTest(level: Loggable.Level.Value): Unit = {
      implicit val logContext = new MockLogContext
    
      var x = 42
    
      assert(x === 42)
    
      quietly("foo", level)(x += 1)
    
      assert(x === 43)
    
      assert(logContext.params() === Vector.empty)
    }
    
    doTest(Loggable.Level.debug)
    doTest(Loggable.Level.error)
    doTest(Loggable.Level.info)
    doTest(Loggable.Level.trace)
    doTest(Loggable.Level.warn)
  }
  
  test("quietly - something thrown") {
    def doTest(level: Loggable.Level.Value): Unit = {
      implicit val logContext = new MockLogContext
    
      var x = 42
    
      assert(x === 42)
    
      val e = new Exception
    
      quietly("foo", level)(throw e)
    
      assert(x === 42)
    
      assert(logContext.params() === Vector((level, "foo", e)))
    }
    
    doTest(Loggable.Level.debug)
    doTest(Loggable.Level.error)
    doTest(Loggable.Level.info)
    doTest(Loggable.Level.trace)
    doTest(Loggable.Level.warn)
  }
}
package loamstream.util

import org.scalatest.FunSuite
import scala.Vector

/**
 * @author clint
 * Dec 7, 2016
 */
final class ThrowablesTest extends FunSuite {
  
  type ParamTuple = (Loggable.Level, String, Throwable)
  
  private final class MockLogContext extends LogContext {
    
    val params: ValueBox[Seq[ParamTuple]] = ValueBox(Vector.empty)
    
    override def log(level: Loggable.Level, s: => String): Unit = ???
    
    override def log(level: Loggable.Level, s: => String, e: Throwable): Unit = {
      params.mutate(_ :+ (level, s, e))
    }
  }
  
  test("collectFailures") {
    import Throwables.collectFailures
    
    assert(collectFailures() === Nil)
    
    assert(collectFailures(() => 42) === Nil)
    
    val e = new Exception 
    
    val f = new Exception 
    
    assert(collectFailures(() => throw e) === Seq(e))
    
    assert(collectFailures(() => 42, () => 42) === Nil)
    
    assert(collectFailures(() => throw e, () => throw f) === Seq(e, f))
    
    assert(collectFailures(() => 42, () => throw e, () => 42, () => throw f) === Seq(e, f))
  }

  test("failureOption") {
    import Throwables.failureOption
    
    assert(failureOption(42) === None)
    
    val e = new Exception 
    
    assert(failureOption(throw e) === Some(e))
  }
  
  import Throwables.quietly
  
  test("quietly - defaults, nothing thrown") {
    implicit val logContext = new MockLogContext
    
    var x = 42
    
    assert(x === 42)
    
    val result = quietly("foo")(x += 1)
    
    assert(result === None)
    
    assert(x === 43)
    
    assert(logContext.params() === Vector.empty)
  }
  
  test("quietly - defaults, something thrown") {
    implicit val logContext = new MockLogContext
    
    var x = 42
    
    assert(x === 42)
    
    val e = new Exception
    
    val result = quietly("foo")(throw e)
    
    assert(result === Some(e))
    
    assert(x === 42)
    
    assert(logContext.params() === Vector((Loggable.Level.Error, "foo", e)))
  }
  
  test("quietly - nothing thrown") {
    def doTest(level: Loggable.Level): Unit = {
      implicit val logContext = new MockLogContext
    
      var x = 42
    
      assert(x === 42)
    
      val result = quietly("foo", level)(x += 1)
    
      assert(result === None)
      
      assert(x === 43)
    
      assert(logContext.params() === Vector.empty)
    }
    
    doTest(Loggable.Level.Debug)
    doTest(Loggable.Level.Error)
    doTest(Loggable.Level.Info)
    doTest(Loggable.Level.Trace)
    doTest(Loggable.Level.Warn)
  }
  
  test("quietly - something thrown") {
    def doTest(level: Loggable.Level): Unit = {
      implicit val logContext = new MockLogContext
    
      var x = 42
    
      assert(x === 42)
    
      val e = new Exception
    
      val result = quietly("foo", level)(throw e)
    
      assert(result === Some(e))
      
      assert(x === 42)
    
      assert(logContext.params() === Vector((level, "foo", e)))
    }
    
    doTest(Loggable.Level.Debug)
    doTest(Loggable.Level.Error)
    doTest(Loggable.Level.Info)
    doTest(Loggable.Level.Trace)
    doTest(Loggable.Level.Warn)
  }
}

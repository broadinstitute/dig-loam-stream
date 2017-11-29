package loamstream.model.jobs.commandline

import scala.sys.process.ProcessLogger
import org.scalatest.FunSuite


/**
 * @author clint
 * Nov 15, 2017
 */
final class CloseableProcessLoggerTest extends FunSuite {
  import CloseableProcessLoggerTest.MockProcessLogger
  
  test("delegate methods") {
    val mockLogger = new MockProcessLogger
    
    val closeable = CloseableProcessLogger(mockLogger) { }
    
    assert(mockLogger.outLines === Nil)
    assert(mockLogger.errLines === Nil)
    assert(mockLogger.bufferedThings === Nil)
    
    closeable.out("foo")
    closeable.out("bar")
    closeable.out("baz")
    
    assert(mockLogger.outLines === Seq("foo", "bar", "baz"))
    assert(mockLogger.errLines === Nil)
    assert(mockLogger.bufferedThings === Nil)
    
    closeable.err("x")
    closeable.err("y")
    closeable.err("z")
    
    assert(mockLogger.outLines === Seq("foo", "bar", "baz"))
    assert(mockLogger.errLines === Seq("x", "y", "z"))
    assert(mockLogger.bufferedThings === Nil)
    
    closeable.buffer(42)
    closeable.buffer(99)
    closeable.buffer(22)
    
    assert(mockLogger.outLines === Seq("foo", "bar", "baz"))
    assert(mockLogger.errLines === Seq("x", "y", "z"))
    assert(mockLogger.bufferedThings === Seq(42, 99, 22))
  }
  
  test("close") {
    val mockLogger = new MockProcessLogger
    
    var closed = false
    
    val closeable = CloseableProcessLogger(mockLogger) { 
      closed = true 
    }
    
    assert(closed === false)
    
    closeable.close()
    
    assert(closed === true)
  }
}

object CloseableProcessLoggerTest {
  private final class MockProcessLogger extends ProcessLogger {
    var outLines: Vector[String] = Vector.empty
    var errLines: Vector[String] = Vector.empty
    var bufferedThings: Vector[Any] = Vector.empty
    
    override def out(s: => String): Unit = outLines :+= s

    override def err(s: => String): Unit = errLines :+= s

    override def buffer[T](f: => T): T = {
      val actualT = f
      
      bufferedThings :+= actualT
      
      actualT
    }
  }
}

package loamstream.util

import org.scalatest.FunSuite

/**
 * @author clint
 * Jun 20, 2019
 */
final class LogContextLevelTest extends FunSuite {
  import LogContext.Level._
  
  test("Names") {
    assert(Trace.name === "Trace")
    assert(Debug.name === "Debug")
    assert(Info.name === "Info")
    assert(Warn.name === "Warn")
    assert(Error.name === "Error")
  }
  
  test(">=") {
    assert(Trace >= Trace)
    assert(Trace >= Debug === false)
    assert(Trace >= Info === false)
    assert(Trace >= Warn === false)
    assert(Trace >= Error === false)
    
    assert(Debug >= Trace)
    assert(Debug >= Debug)
    assert(Debug >= Info === false)
    assert(Debug >= Warn === false)
    assert(Debug >= Error === false)
    
    assert(Info >= Trace)
    assert(Info >= Debug)
    assert(Info >= Info)
    assert(Info >= Warn === false)
    assert(Info >= Error === false)
    
    assert(Warn >= Trace)
    assert(Warn >= Debug)
    assert(Warn >= Info)
    assert(Warn >= Warn)
    assert(Warn >= Error === false)
    
    assert(Error >= Trace)
    assert(Error >= Debug)
    assert(Error >= Info)
    assert(Error >= Warn)
    assert(Error >= Error)
  }
}

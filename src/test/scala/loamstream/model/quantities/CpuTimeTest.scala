package loamstream.model.quantities

import org.scalatest.FunSuite
import scala.concurrent.duration.DurationDouble
import scala.concurrent.duration.DurationInt
import org.scalactic.TripleEqualsSupport

/**
 * @author clint
 * Mar 13, 2017
 */
final class CpuTimeTest extends FunSuite {
  import scala.concurrent.duration._
  
  test("seconds") {
    assert(CpuTime(1.minute).seconds === 60.0)
    
    assert(CpuTime(0.5.minutes).seconds === 30.0)
    
    def doRoundTrip(s: Double): Unit = {
      assert(CpuTime(s.seconds).seconds === s)
    }
    
    doRoundTrip(0.25)
    doRoundTrip(0.0)
    doRoundTrip(1.25)
    doRoundTrip(42.0)
  }
  
  test("hours") {
    
    assert(CpuTime(240.minutes).hours === 4.0)
    assert(CpuTime(1.hour).hours === 1.0)
    
    def doRoundTrip(s: Double): Unit = {
      assert(CpuTime(s.hours).hours === s)
    }
    
    doRoundTrip(0.25)
    doRoundTrip(0.0)
    doRoundTrip(1.25)
    doRoundTrip(42.0)
  }
  
  test("inSeconds") {
    def doRoundTrip(d: Double): Unit = {
      import TripleEqualsSupport.Spread
      
      assert(CpuTime.inSeconds(d).seconds === Spread(d, 0.0001))
    }
    
    doRoundTrip(0.0)
    doRoundTrip(0.1)
    doRoundTrip(42.0)
    doRoundTrip(Double.MinPositiveValue)
    
    intercept[Exception] {
      CpuTime.inSeconds(-0.1)
    }
    
    intercept[Exception] {
      CpuTime.inSeconds(-42.0)
    }
  }
  
  test("inHours") {
    def doRoundTrip(d: Double): Unit = {
      import TripleEqualsSupport.Spread
      
      assert(CpuTime.inHours(d).hours === Spread(d, 0.0001))
    }
    
    doRoundTrip(0.0)
    doRoundTrip(0.1)
    doRoundTrip(42.0)
    doRoundTrip(Double.MinPositiveValue)
    
    intercept[Exception] {
      CpuTime.inHours(-0.1)
    }
    
    intercept[Exception] {
      CpuTime.inHours(-42.0)
    }
  }
}

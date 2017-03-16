package loamstream.model.execute

import org.scalatest.FunSuite
import scala.concurrent.duration.DurationDouble
import scala.concurrent.duration.DurationInt

/**
 * @author clint
 * Mar 13, 2017
 */
final class CpuTimeTest extends FunSuite {
  test("seconds") {
    import scala.concurrent.duration._
    
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
}

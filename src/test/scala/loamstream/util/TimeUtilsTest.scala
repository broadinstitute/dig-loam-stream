package loamstream.util

import org.scalatest.FunSuite
import scala.util.Success
import scala.util.Failure

/**
 * @author clint
 * Mar 28, 2017
 */
final class TimeUtilsTest extends FunSuite {
  //scalastyle:off magic.number
  test("startAndEndTime") {
    val (attempt, (start, end)) = TimeUtils.startAndEndTime { 42 }
    
    assert(attempt === Success(42))
    assert(start.toEpochMilli <= end.toEpochMilli)
  }
  
  test("startAndEndTime - exception") {
    val e = new Exception
    
    val (attempt, (start, end)) = TimeUtils.startAndEndTime { throw e }
    
    assert(attempt === Failure(e))
    assert(start.toEpochMilli <= end.toEpochMilli)
  }
  //scalastyle:on magic.number
}

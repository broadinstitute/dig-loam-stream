package loamstream.util

import org.scalatest.FunSuite
import scala.util.Try

/**
 * @author clint
 * date: Jul 19, 2016
 */
final class ReflectionUtilTest extends FunSuite {
  import ReflectionUtil.getObject
  
  private def classLoader = getClass.getClassLoader
  
  test("getObject") {
    assert(getObject[Try.type](classLoader, "scala.util.Try") eq Try)
    
    assert(getObject[ReflectionUtilTest.type](classLoader, "loamstream.util.ReflectionUtilTest") eq ReflectionUtilTest)
  }
}

object ReflectionUtilTest
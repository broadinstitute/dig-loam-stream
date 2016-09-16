package loamstream.util

import loamstream.util.code.TypeName
import org.scalatest.FunSuite

import scala.util.Try

/**
 * @author clint
 * date: Jul 19, 2016
 */
final class ReflectionUtilTest extends FunSuite {
  import loamstream.util.code.ReflectionUtil.getObject
  
  private def classLoader = getClass.getClassLoader
  
  test("getObject") {
    assert(getObject[Try.type](classLoader, TypeName("scala", "util", "Try")) eq Try)
    
    assert(getObject[ReflectionUtilTest.type](classLoader,
      TypeName("loamstream", "util", "ReflectionUtilTest")) eq ReflectionUtilTest)
  }
}

object ReflectionUtilTest
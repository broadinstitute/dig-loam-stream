package loamstream.util

import loamstream.util.code.ObjectId
import org.scalatest.FunSuite

import scala.util.Try

/**
  * @author clint
  *         date: Jul 19, 2016
  */
final class ReflectionUtilTest extends FunSuite {

  import loamstream.util.code.ReflectionUtil.getObject

  private def classLoader = getClass.getClassLoader

  test("getObject") {
    assert(getObject[Try.type](classLoader, ObjectId("scala", "util", "Try")) eq Try)

    assert(getObject[ReflectionUtilTest.type](classLoader,
      ObjectId("loamstream", "util", "ReflectionUtilTest")) eq ReflectionUtilTest)
  }
}

object ReflectionUtilTest
package loamstream.util

import loamstream.util.OsName.Family
import org.scalatest.FunSuite

/**
  * LoamStream
  * Created by oliverr on 9/21/2016.
  */
class OsNameTest extends FunSuite {

  def osNames(name: String, names: String*): Seq[OsName] = (name +: names).map(OsName.apply)

  test("Current OS is detected, known and properly described") {
    val currentOsName = OsName.current
    assert(currentOsName.name == scala.sys.props(OsName.propKey))
    assert(currentOsName.familyShot.nonEmpty)
  }

  test("Identify various names as Windows") {
    for (osName <- osNames("Windows XP", "Windows Blub", "Windows 10", "Super Windows")) {
      assert(osName.familyShot.contains(Family.windows))
      assert(osName.isWindows)
      assert(!osName.isUnixLike)
      assert(!osName.isMacOsX)
      assert(!osName.isOtherMac)
      assert(!osName.isMac)
    }
  }

  test("Identify various names as Unix-like") {
    for (osName <- osNames("Linux", "SunOS", "AIX", "HP-UX", "Solaris", "Minix", "Yunux")) {
      assert(osName.familyShot.contains(Family.unixLike))
      assert(!osName.isWindows)
      assert(osName.isUnixLike)
      assert(!osName.isMacOsX)
      assert(!osName.isOtherMac)
      assert(!osName.isMac)
    }
  }

  test("Identify Mac OS X") {
    val osName = OsName("Mac OS X")
    assert(osName.familyShot.contains(Family.unixLike))
    assert(!osName.isWindows)
    assert(osName.isUnixLike)
    assert(osName.isMacOsX)
    assert(!osName.isOtherMac)
    assert(osName.isMac)
  }

  test("Identify various names as other Mac versions") {
    for (osName <- osNames("Mac Old", "Mac 1", "Mac whatever")) {
      assert(osName.familyShot.contains(Family.otherMac))
      assert(!osName.isWindows)
      assert(!osName.isUnixLike)
      assert(!osName.isMacOsX)
      assert(osName.isOtherMac)
      assert(osName.isMac)
    }
  }

  test("Categorize certain unknown names as neither Windows, nor Unix-like, nor Mac") {
    for (osName <- osNames("Wubbzy OS", "OS/2", "Zuse 2", "Atari TOS", "DOS")) {
      assert(osName.familyShot.isEmpty)
      assert(!osName.isWindows)
      assert(!osName.isUnixLike)
      assert(!osName.isMacOsX)
      assert(!osName.isOtherMac)
      assert(!osName.isMac)
    }
  }

}

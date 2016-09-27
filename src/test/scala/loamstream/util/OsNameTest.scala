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
    assert(currentOsName.newlineShot.contains(System.lineSeparator))
  }

  def assertShot[T](shot: Shot[T], opt: Option[T]): Unit = opt match {
    case Some(value) => assert(shot.contains(value))
    case None => assert(shot.isEmpty)
  }

  def assertFamily(osName: OsName, familyOpt: Option[Family], isWindows: Boolean, isUnixLike: Boolean,
                   newlineOpt: Option[String], intraPathSepOpt: Option[String],
                   interPathSepOpt: Option[String]): Unit = {
    assertShot(osName.familyShot, familyOpt)
    assert(osName.isWindows === isWindows)
    assert(osName.isUnixLike === isUnixLike)
    assertShot(osName.newlineShot, newlineOpt)
    assertShot(osName.intraPathSepShot, intraPathSepOpt)
    assertShot(osName.interPathSepShot, interPathSepOpt)
  }


  test("Identify various names as Windows") {
    for (osName <- osNames("Windows XP", "Windows Blub", "Windows 10", "Super Windows")) {
      assertFamily(osName, Some(Family.windows), isWindows = true, isUnixLike = false,
        Some("\r\n"), Some("\\"), Some(";"))
    }
  }

  test("Identify various names as Unix-like") {
    for (osName <- osNames("Linux", "Mac OS X", "SunOS", "AIX", "HP-UX", "Solaris", "Minix", "Yunux")) {
      assertFamily(osName, Some(Family.unixLike), isWindows = false, isUnixLike = true,
        Some("\n"), Some("/"), Some(":"))
    }
  }

  test("Categorize certain unknown names as neither Windows, nor Unix-like, nor Mac") {
    for (osName <- osNames("Wubbzy OS", "OS/2", "Zuse 2", "Atari TOS", "DOS")) {
      assertFamily(osName, None, isWindows = false, isUnixLike = false, None, None, None)
    }
  }

}

package loamstream.util

import loamstream.util.OsName.Family

/** Identifies platform, e.g. whether Windows or Linux */
case class OsName(name: String) {

  /** Os family: Windows, UnixLike, OtherMac or Unknown */
  def family: Shot[Family] = {
    if (Family.windows.knownNames.contains(this)) {
      Hit(OsName.Family.windows)
    } else if (Family.unixLike.knownNames.contains(this)) {
      Hit(OsName.Family.unixLike)
    } else if (Family.otherMac.knownNames.contains(this)) {
      Hit(OsName.Family.otherMac)
    } else if (Family.windows.nameClues.exists(name.contains)) {
      Hit(OsName.Family.windows)
    } else if (Family.unixLike.nameClues.exists(name.contains)) {
      Hit(OsName.Family.unixLike)
    } else if (Family.otherMac.nameClues.exists(name.contains)) {
      Hit(OsName.Family.otherMac)
    } else {
      Miss(s"Unknown OS name $name")
    }
  }

  /** Whether this is Windows */
  def isWindows: Boolean = family == Hit(Family.windows)

  /** Whether this is Unix-like (including Mac OS X) */
  def isUnixLike: Boolean = family == Hit(Family.unixLike)

  /** Whether this is Mac OS X */
  def isMacOsX: Boolean = this == OsName.macOsX

  /** Whether this is Mac other than Mac OS X */
  def isOtherMac: Boolean = family == Hit(Family.otherMac)

  /** Whether this is Mac (including Mac OS X) */
  def isMac: Boolean = isMacOsX || isOtherMac

}

/** Identifies platform, e.g. whether Windows or Linux */
object OsName {

  def current: OsName = OsName(scala.sys.props("os.name"))

  val macOsX: OsName = OsName("Mac OS X")

  case class Family(knownNames: Set[OsName], nameClues: Set[String])

  object Family {

    val windows = Family(
      knownNames =
        Set("98", "XP", "NT", "Me", "2000", "2003", "Vista", "7", "10").map(suffix => s"Windows $suffix")
          .map(OsName.apply),
      nameClues = Set("Windows")
    )

    val unixLike = Family(
      knownNames = Set("Linux", "SunOS", "FreeBSD").map(OsName.apply) + macOsX,
      nameClues = Set("nix", "nux", "AIX", "UX", "Solaris")
    )

    val otherMac = Family(
      knownNames = Set(),
      nameClues = Set("Mac")
    )

  }

}
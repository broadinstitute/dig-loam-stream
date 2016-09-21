package loamstream.util

import loamstream.util.OsName.Family

/** Identifies platform, e.g. whether Windows or Linux */
case class OsName(name: String) {

  /** Os family shot: Windows, UnixLike, OtherMac */
  def familyShot: Shot[Family] = {
    val familyOpt =
      Family.all.find(_.knownNames.contains(this)).orElse(Family.all.find(_.nameClues.exists(name.contains(_))))
    Shot.fromOption(familyOpt, Snag(s"Unknown OS name $name"))
  }

  /** Whether this is Windows */
  def isWindows: Boolean = familyShot.contains(Family.windows)

  /** Whether this is Unix-like (including Mac OS X) */
  def isUnixLike: Boolean = familyShot.contains(Family.unixLike)

  /** Whether this is Mac OS X */
  def isMacOsX: Boolean = this == OsName.macOsX

  /** Whether this is Mac other than Mac OS X */
  def isOtherMac: Boolean = familyShot.contains(Family.otherMac)

  /** Whether this is Mac (including Mac OS X) */
  def isMac: Boolean = isMacOsX || isOtherMac

}

/** Identifies platform, e.g. whether Windows or Linux */
object OsName {

  val propKey: String = "os.name"

  def current: OsName = OsName(scala.sys.props(propKey))

  val macOsX: OsName = OsName("Mac OS X")

  case class Family private(knownNames: Set[OsName], nameClues: Set[String]) {
    Family._all += this
  }

  object Family {

    private var _all: Set[Family] = Set.empty

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

    val all = _all

  }

}
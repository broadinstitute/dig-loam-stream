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

  /** Shot of the newline String */
  def newlineShot: Shot[String] = familyShot.map(_.newline)

  /** Shot of the separator inside paths  */
  def intraPathSepShot: Shot[String] = familyShot.map(_.intraPathSep)

  /** Shot of the separator between paths in PATH variable */
  def interPathSepShot: Shot[String] = familyShot.map(_.interPathSep)

}

/** Identifies platform, e.g. whether Windows or Linux */
object OsName {

  val propKey: String = "os.name"

  def current: OsName = OsName(scala.sys.props(propKey))

  case class Family private(knownNames: Set[OsName], nameClues: Set[String], newline: String,
                            intraPathSep: String, interPathSep: String) {
    Family._all += this
  }

  object Family {

    private var _all: Set[Family] = Set.empty

    val windows = Family(
      knownNames =
        Set("98", "XP", "NT", "Me", "2000", "2003", "Vista", "7", "10").map(suffix => s"Windows $suffix")
          .map(OsName.apply),
      nameClues = Set("Windows"),
      newline = "\r\n",
      intraPathSep = "\\",
      interPathSep = ";"
    )

    val unixLike = Family(
      knownNames = Set("Linux", "Mac OS X", "SunOS", "FreeBSD").map(OsName.apply),
      nameClues = Set("nix", "nux", "AIX", "UX", "Solaris"),
      newline = "\n",
      intraPathSep = "/",
      interPathSep = ":"
    )

    val all = _all

  }

}
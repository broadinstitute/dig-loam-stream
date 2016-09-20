package loamstream.util

import loamstream.util.OsInfo.Family
import loamstream.util.OsInfo.Family.{OtherMac, UnixLike, Unknown, Windows}

/** Identifies platform, e.g. whether Windows or Linux */
case class OsInfo(family: Family, name: String) {


}

/** Identifies platform, e.g. whether Windows or Linux */
object OsInfo {

  sealed trait Family

  object Family {

    case object Windows extends Family

    case object UnixLike extends Family

    case object OtherMac extends Family

    case object Unknown extends Family

  }

  val windows98 = OsInfo(Windows, "Windows 98")
  val windowsXP = OsInfo(Windows, "Windows XP")
  val windowsNT = OsInfo(Windows, "Windows NT")
  val windowsMe = OsInfo(Windows, "Windows Me")
  val windows2000 = OsInfo(Windows, "Windows 2000")
  val windows2003 = OsInfo(Windows, "Windows 2003")
  val windowsVista = OsInfo(Windows, "Windows Vista")
  val windows7 = OsInfo(Windows, "Windows 7")
  val windows10 = OsInfo(Windows, "Windows 10")
  val windowsOther = OsInfo(Windows, "Windows")
  val linux = OsInfo(UnixLike, "Linux")
  val macOsX = OsInfo(UnixLike, "Mac OS X")
  val macOther = OsInfo(OtherMac, "Mac Other")
  val sunOs = OsInfo(UnixLike, "SunOS")
  val FreeBsd = OsInfo(UnixLike, "FreeBSD")
  val unixLikeOther = OsInfo(UnixLike, "Unix-like")
  val unknown = OsInfo(Unknown, "Unknown")

  val all: Set[OsInfo] = Set(windows98, windowsXP, windowsNT, windowsMe, windows2000, windows2003, windows7,
    windows10, windowsOther, linux, macOsX, unixLikeOther)
  val dir: Map[String, OsInfo] = all.map(osInfo => (osInfo.name, osInfo)).toMap

  def current: OsInfo = {
    val osName = scala.sys.props("os.name")
    val osNameLow = osName.toLowerCase
    val osInfoOpt = dir.get(osName)
    if (osInfoOpt.nonEmpty) {
      osInfoOpt.get
    } else if (osName.contains("Windows")) {
      windowsOther
    } else if (osName.contains("Mac")) {
      macOther
    } else if (osNameLow.contains("nix") || osNameLow.contains("nux") || osNameLow.contains("bsd") ||
      osNameLow.contains("aix")) {
      unixLikeOther
    } else {
      unknown
    }
  }

}
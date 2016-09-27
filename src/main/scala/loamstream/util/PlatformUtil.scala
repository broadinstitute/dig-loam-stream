package loamstream.util

/** Platform related utility */
object PlatformUtil {

  /** Whether we are running MS Windows */
  def isWindows: Boolean = scala.sys.props("os.name").contains("Windows")

}

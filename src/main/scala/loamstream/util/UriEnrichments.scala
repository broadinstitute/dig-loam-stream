package loamstream.util

import java.net.URI

/**
  * @author kyuksel
  * date: Nov 15, 2016
  */
object UriEnrichments {
  /**
   * Append a slash to the end of a URI if it does not already end with one
   */
  def appendSlashIfNoneExists(uri: URI): URI = {
    val uriStr = uri.toString

    if (uriStr.endsWith("/")) { uri }
    else { URI.create(uriStr + "/") }
  }

  final implicit class UriHelpers(val uri: URI) extends AnyVal {
    /**
     * The important difference between this method and java.net.URI.resolve(String) is in the treatment of the final
     * segment. The URI resolve method drops the last segment if there is no trailing slash as specified in section 5.2
     * of RFC 2396. This leads to unpredictable behaviour when working with file: URIs, because the existence of the
     * trailing slash depends on the existence of a local file on disk. This method operates like a traditional path
     * append and always preserves all segments of the base path.
     *
     * @param next URI segment(s) to be appended
     * @return A new URI with all the same components as the given base URI, but with a path component
     * created by appending the given extension to the base URI's path.
     */
    def append(next: String): URI = appendSlashIfNoneExists(uri).resolve(next)

    def /(next: String): URI = append(next)
  }
  
}

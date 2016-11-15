package loamstream.util

import java.net.URI
import scala.util.Try

/**
  * @author kyuksel
  *         date: Nov 15, 2016
  */
object UriEnrichments {

  final implicit class UriHelpers(val uri: URI) extends AnyVal {
    def /(next: String): URI = uri.resolve(next)
    
    def /(next: Try[String]): Try[URI] = next.map(/)
  }
  
  final implicit class PathAttemptHelpers(val attempt: Try[URI]) extends AnyVal {
    def /(next: String): Try[URI] = attempt.map(_ / next)
    
    def /(next: Try[String]): Try[URI] = attempt.flatMap(_ / next)
  }

}
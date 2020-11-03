package loamstream.util

/**
 * @author clint
 * Mar 31, 2020
 */
trait HttpClient {
  def get(url: String): Either[String, String]
  
  def contentLength(url: String): Either[String, Long]
}

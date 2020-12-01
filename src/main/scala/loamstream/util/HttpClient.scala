package loamstream.util

/**
 * @author clint
 * Mar 31, 2020
 */
trait HttpClient {
  def get(url: String): Either[String, String]
  
  def contentLength(url: String): Either[String, Long]
  
  def post(url: String, body: Option[String] = None, headers: Map[String, String] = Map.empty): Either[String, String]
}

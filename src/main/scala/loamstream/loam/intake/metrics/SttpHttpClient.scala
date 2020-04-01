package loamstream.loam.intake.metrics

import loamstream.util.Loggable
import sttp.client.HttpURLConnectionBackend
import sttp.client.basicRequest
import sttp.client.UriContext

/**
 * @author clint
 * Mar 31, 2020
 */
final class SttpHttpClient extends HttpClient with Loggable {
  private val backend = HttpURLConnectionBackend()
  
  override def get(url: String): Either[String, String] = {
    trace(s"Invoking $url")
    
    val request = basicRequest.get(uri"${url}")
    
    backend.send(request).body 
  }
}

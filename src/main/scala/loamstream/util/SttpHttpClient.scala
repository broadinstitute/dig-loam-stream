package loamstream.util

import sttp.client.HttpURLConnectionBackend
import sttp.client.basicRequest
import sttp.client.asStream
import sttp.client.UriContext

/**
 * @author clint
 * Mar 31, 2020
 */
final class SttpHttpClient extends HttpClient with Loggable {
  private val backend = HttpURLConnectionBackend()
  
  override def get(url: String): Either[String, String] = {
    trace(s"Invoking GET $url")
    
    val request = basicRequest.get(uri"${url}")
    
    backend.send(request).body 
  }
  
  override def contentLength(url: String): Either[String, Long] = {
    trace(s"Invoking HEAD $url")
    
    val request = basicRequest.head(uri"${url}")
    
    backend.send(request).contentLength.toRight(s"No content-length returned when invoking HEAD ${url}")
  }
  
  override def post(
      url: String, 
      body: Option[String] = None, 
      headers: Map[String, String] = Map.empty): Either[String, String] = {
    
    trace(s"Invoking POST ${url} ; headers = ${headers} ; body size: ${body.map(_.size)}")
    
    val request = basicRequest.post(uri"${url}").body(body.getOrElse("")).headers(headers)
    
    val response = backend.send(request)
    
    response.body
  }
}

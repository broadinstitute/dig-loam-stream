package loamstream.util

import sttp.client._
import java.io.InputStream
import scala.util.control.NonFatal

/**
 * @author clint
 * Mar 31, 2020
 */
final class SttpHttpClient extends HttpClient with Loggable {
  private val backend = HttpURLConnectionBackend()
  
  import SttpHttpClient.RequestOps
  
  private def doGetRequest[A](
      url: String, 
      auth: Option[HttpClient.Auth], 
      asType: ResponseAs[Either[String, A], Nothing]): Either[String, A] = {
    
    trace(s"Invoking GET $url")
    
    val request = basicRequest.get(uri"${url}").withAuth(auth).response(asType)
    
    backend.send(request).body
  }
  
  override def get(url: String, auth: Option[HttpClient.Auth] = None): Either[String, String] = {
    doGetRequest(url, auth, asString)
  }
  
  override def getAsBytes(url: String, auth: Option[HttpClient.Auth] = None): Either[String, Array[Byte]] = {
    doGetRequest(url, auth, asByteArray)
  }
  
  override def getAsInputStream(url: String, auth: Option[HttpClient.Auth] = None): Either[String, InputStream] = {
    try {
      Right {
        requests.get.stream(url).readBytesThrough(identity)
      }
    } catch {
      case NonFatal(e) => Left(s"Couldn't download '${url}': ${e.getMessage}")
    }
  }
  
  override def contentLength(url: String, auth: Option[HttpClient.Auth] = None): Either[String, Long] = {
    trace(s"Invoking HEAD $url")
    
    val request = basicRequest.head(uri"${url}").withAuth(auth)
    
    backend.send(request).contentLength.toRight(s"No content-length returned when invoking HEAD ${url}")
  }
  
  override def post(
      url: String, 
      body: Option[String] = None, 
      headers: Map[String, String] = Map.empty,
      auth: Option[HttpClient.Auth] = None): Either[String, String] = {
    
    trace(s"Invoking POST ${url} ; headers = ${headers} ; body size: ${body.map(_.size)}")
    
    val request = basicRequest.post(uri"${url}").body(body.getOrElse("")).headers(headers).withAuth(auth)
    
    val response = backend.send(request)
    
    response.body
  }
}

object SttpHttpClient {
  private final implicit class RequestOps[A, B](val req: Request[A, B]) extends AnyVal {
    def withAuth(authOpt: Option[HttpClient.Auth]): Request[A, B] = authOpt match {
      case Some(HttpClient.Auth(username, password)) => req.auth.basic(username, password)
      case _ => req
    }
  }
}

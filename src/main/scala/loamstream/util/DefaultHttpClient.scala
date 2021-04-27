package loamstream.util

import java.io.InputStream
import scala.util.control.NonFatal
import requests.RequestAuth
import requests.Response

/**
 * @author clint
 * Mar 31, 2020
 */
final class DefaultHttpClient extends HttpClient with Loggable {
  private val backend = sttp.client.HttpURLConnectionBackend()
  
  import DefaultHttpClient._
  
  private def doGetRequest[A](
      url: String, 
      auth: Option[HttpClient.Auth], 
      asType: Response => A): Either[String, A] = {
    
    trace(s"Invoking GET $url")
    
    attempt(url) {
      asType(requests.get(url = url, auth = toRequestAuth(auth)))
    }
  }
  
  override def get(url: String, auth: Option[HttpClient.Auth] = None): Either[String, String] = {
    doGetRequest(url, auth, _.text)
  }
  
  override def getAsBytes(url: String, auth: Option[HttpClient.Auth] = None): Either[String, Array[Byte]] = {
    doGetRequest(url, auth, _.bytes)
  }
  
  override def getAsInputStream(url: String, auth: Option[HttpClient.Auth] = None): Either[String, InputStream] = {
    attempt(url) {
      //TODO: Specifying auth leads to 400 errors :( 
      requests.get.stream(url = url/*, auth = toRequestAuth(auth)*/).readBytesThrough(identity)
    }
  }
  
  override def contentLength(url: String, auth: Option[HttpClient.Auth] = None): Either[String, Long] = {
    trace(s"Invoking HEAD $url")

    //NB: Use Sttp for this, since Requests-Scala was downloading the entire URL
    import sttp.client._
    
    val request = basicRequest.head(uri"${url}").withAuth(auth)
    
    backend.send(request).contentLength.toRight(s"No content-length returned when invoking HEAD ${url}")
  }
  
  override def post(
      url: String, 
      body: Option[String] = None, 
      headers: Map[String, String] = Map.empty,
      auth: Option[HttpClient.Auth] = None): Either[String, String] = {
    
    trace(s"Invoking POST ${url} ; headers = ${headers} ; body size: ${body.map(_.size)}")
    
    attempt(url) {
      requests.post(
        url = url, 
        auth = toRequestAuth(auth), 
        data = body.getOrElse("").getBytes,
        headers = headers).text
    }
  }
  
  private def attempt[A](url: String)(a: => A): Either[String, A] = {
    try {
      Right(a)
    } catch {
      case NonFatal(e) => Left(s"Couldn't download '${url}': ${e.getMessage}")
    }
  }
}

object DefaultHttpClient {
  private def toRequestAuth(oa: Option[HttpClient.Auth]): RequestAuth = oa match {
    case Some(a) => new RequestAuth.Basic(username = a.username, password = a.password)
    case None => RequestAuth.Empty
  }
  
  private final implicit class RequestOps[A, B](val req: sttp.client.Request[A, B]) extends AnyVal {
    def withAuth(authOpt: Option[HttpClient.Auth]): sttp.client.Request[A, B] = authOpt match {
      case Some(HttpClient.Auth(username, password)) => req.auth.basic(username, password)
      case _ => req
    }
  }
}

package loamstream.util

import java.io.InputStream
import scala.util.control.NonFatal
import requests.RequestAuth
import requests.Response
import java.io.ByteArrayInputStream

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
  
  private object OkHttp {
    val authenticator: okhttp3.Authenticator = { (route: okhttp3.Route, response: okhttp3.Response) =>
      response.request.newBuilder
        .removeHeader("Accept-Encoding")
        .header("Accept-Encoding", "gzip")
        .build
    }
  }
  
  override def getAsInputStream(url: String, auth: Option[HttpClient.Auth] = None): Either[String, InputStream] = {
    trace(s"Invoking GET $url")
    
    attempt(url) {
      val client = (new okhttp3.OkHttpClient.Builder).authenticator(OkHttp.authenticator).build
      
      val baseReqBuilder = (new okhttp3.Request.Builder)
          .url(url)
          .removeHeader("Accept-Encoding")
          .header("Accept-Encoding", "identity")
      
      val reqBuilder = auth match {
        case Some(a) => {
          baseReqBuilder
            .header("Authorization", okhttp3.Credentials.basic(a.username, a.password))
            //NB: disable OkHttp's automagic handling of gzipped data, so we can get the actual 
            //.bed.gz files from DGA, for example
            .removeHeader("Accept-Encoding")
            .header("Accept-Encoding", "identity")
        }
        case None => baseReqBuilder
      }
      
      val request = reqBuilder.build
      
      val response = client.newCall(request).execute()
        
      response.body.byteStream
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

package loamstream.util

import java.io.InputStream

import HttpClient.Auth

/**
 * @author clint
 * Mar 31, 2020
 */
trait HttpClient {
  def get(url: String, auth: Option[Auth] = None): Either[String, String]
  
  def getAsBytes(url: String, auth: Option[HttpClient.Auth] = None): Either[String, Array[Byte]]
  
  def getAsInputStream(url: String, auth: Option[HttpClient.Auth] = None): Either[String, (Terminable, InputStream)]
  
  def contentLength(url: String, auth: Option[Auth] = None): Either[String, Long]
  
  def post(
      url: String, 
      body: Option[String] = None, 
      headers: Map[String, String] = Map.empty,
      auth: Option[Auth] = None): Either[String, String]
}

object HttpClient {
  /**
   * @author clint
   * Jan 20, 2021
   */
  final case class Auth(username: String, password: String)
}

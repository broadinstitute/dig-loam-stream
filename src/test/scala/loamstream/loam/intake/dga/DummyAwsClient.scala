package loamstream.loam.intake.dga

import loamstream.util.AwsClient
import loamstream.util.AwsClient.ContentType

/**
 * @author clint
 * May 5, 2021
 */
object DummyAwsClient extends AwsClient {
  override def bucket: String = ???
  
  override def list(prefix: String, delimiter: String = "/"): Seq[String] = ???
  
  override def deleteDir(key: String): Unit = ???
  
  override def put(key: String, data: String, contentType: Option[ContentType] = None): Unit = ???
  
  override def getAsString(key: String): Option[String] = ???
}
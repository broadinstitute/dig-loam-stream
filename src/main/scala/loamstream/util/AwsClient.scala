package loamstream.util

import org.broadinstitute.dig.aws.AWS

import AwsClient.ContentType
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.services.s3.model.NoSuchKeyException
import software.amazon.awssdk.services.s3.model.PutObjectRequest

/**
 * @author clint
 * Dec 7, 2020
 */
trait AwsClient {
  def bucket: String
  
  def list(prefix: String, delimiter: String): Seq[String]
  
  def deleteDir(key: String): Unit
  
  def put(key: String, data: String, contentType: Option[ContentType] = None): Unit
  
  def getAsString(key: String): Option[String]
}

object AwsClient {
  sealed abstract class ContentType(val value: String)
  
  object ContentType {
    case object ApplicationJson extends ContentType("application/json") 
  }
  
  final class Default(aws: AWS) extends AwsClient {
    override def bucket: String = aws.bucket
    
    override def list(prefix: String, delimiter: String): Seq[String] = {
      aws.ls(prefix).unsafeRunSync()
    }
  
    override def deleteDir(key: String): Unit = {
      aws.rmdir(key).unsafeRunSync()
    }
  
    override def put(key: String, data: String, contentType: Option[ContentType] = None): Unit = {
      val requestBody: RequestBody = RequestBody.fromString(data)
      
      val baseBuilder = PutObjectRequest.builder.bucket(bucket).key(key)
     
      val reqBuilder = contentType match {
        case Some(ct) => baseBuilder.contentType(ct.value)
        case _ => baseBuilder
      }
      
      val req = reqBuilder.build
      
      aws.s3.putObject(req, requestBody)
    }
    
    override def getAsString(key: String): Option[String] = {
      import org.broadinstitute.dig.aws.Implicits._
      
      try {
        Some(aws.get(key).map(_.readAsString()).unsafeRunSync())
      } catch {
        case _: NoSuchKeyException => None
      }
    }
  }
}

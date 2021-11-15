package loamstream.util

import org.broadinstitute.dig.aws.S3

import S3Client.ContentType
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.services.s3.model.NoSuchKeyException
import software.amazon.awssdk.services.s3.model.PutObjectRequest

/**
 * @author clint
 * Dec 7, 2020
 */
trait S3Client {
  def bucket: String
  
  def list(prefix: String, delimiter: String = "/"): Seq[String]
  
  def deleteDir(key: String): Unit
  
  def put(key: String, data: String, contentType: Option[ContentType] = None): Unit
  
  def getAsString(key: String): Option[String]
}

object S3Client {
  sealed abstract class ContentType(val value: String)
  
  object ContentType {
    case object ApplicationJson extends ContentType("application/json") 
  }
  
  final class Default private (bucketApi: S3.Bucket) extends S3Client {
    def this(bucketName: String) = this(new S3.Bucket(bucketName))
    
    override def bucket: String = bucketApi.bucket
    
    override def list(prefix: String, delimiter: String = "/"): Seq[String] = {
      bucketApi.ls(prefix).map(_.key)
    }
  
    override def deleteDir(key: String): Unit = {
      bucketApi.rm(key)
    }
  
    override def put(key: String, data: String, contentType: Option[ContentType] = None): Unit = {
      val requestBody: RequestBody = RequestBody.fromString(data)
      
      val baseBuilder = PutObjectRequest.builder.bucket(bucket).key(key)
     
      val reqBuilder = contentType match {
        case Some(ct) => baseBuilder.contentType(ct.value)
        case _ => baseBuilder
      }
      
      val req = reqBuilder.build
      
      bucketApi.put(key, data)
    }
    
    override def getAsString(key: String): Option[String] = {
      import org.broadinstitute.dig.aws.Implicits._
      
      try {
        Some(bucketApi.get(key).mkString())
      } catch {
        case _: NoSuchKeyException => None
      }
    }
  }
}

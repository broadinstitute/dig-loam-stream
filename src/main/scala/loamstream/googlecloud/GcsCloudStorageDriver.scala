package loamstream.googlecloud

import java.net.URI
import java.nio.file.Files
import java.nio.file.Path

import scala.util.Success
import scala.util.Try

import com.google.auth.oauth2.ServiceAccountCredentials
import com.google.cloud.storage.Storage
import com.google.cloud.storage.Storage.BlobField
import com.google.cloud.storage.Storage.BlobListOption
import com.google.cloud.storage.StorageException
import com.google.cloud.storage.StorageOptions

import loamstream.util.Loggable
import loamstream.util.Tries
import com.google.cloud.storage.BlobId
import com.google.cloud.storage.BlobInfo
import com.google.cloud.storage.Blob

/**
 * @author kyuksel
 *         date: 3/5/17
 */
final case class GcsCloudStorageDriver(credentialsFile: Path) extends CloudStorageDriver with Loggable {
  import loamstream.util.Uris.Implicits._
  import scala.jdk.CollectionConverters._
  
  /**
   * Instantiate a GCS handle using given credentials.
   * If no credentials provided, attempt to find credentials
   * that might be set in the environment.
   */
  private lazy val storage: Storage = {
    val credentials = ServiceAccountCredentials.fromStream(java.nio.file.Files.newInputStream(credentialsFile))
    
    StorageOptions.newBuilder.setCredentials(credentials).build.getService
  }

  // The name for the bucket
  private[googlecloud] def bucketName(uri: URI): String = uri.getHost

  /**
   * Return the object (blob) 'uri' identifies.
   * If 'uri' is a directory, return the list of all objects (blobs)
   * underneath it (directly or recursively).
   */
  override def blobsAt(uri: URI): Iterable[BlobMetadata] = {
    try {
      val withPrefix = BlobListOption.prefix(uri.getPathWithoutLeadingSlash)
      val withRelevantFields = BlobListOption.fields(
        BlobField.NAME,
        BlobField.UPDATED,
        BlobField.MD5HASH)

      val page = storage.list(bucketName(uri), withPrefix, withRelevantFields)
      
      def matchesIgnoringTrailingSlashes(b: Blob): Boolean = {
        def withoutTrailingSlash(s: String): String = if(s.endsWith("/")) s.dropRight(1) else s

        import java.nio.file.Paths.{get => path}
        
        path(withoutTrailingSlash(b.getName)).startsWith(path(withoutTrailingSlash(uri.getPathWithoutLeadingSlash)))
      }
      
      //Note iterateAll, to iterate (potentially) over every Page of results, not just the first one
      def resultIterator = page.iterateAll.iterator.asScala.filter(matchesIgnoringTrailingSlashes).map { b => 
        BlobMetadata(b.getName, b.getMd5, b.getUpdateTime)
      }
      
      LazyIterable(resultIterator)
    } catch {
      case e: StorageException =>
        warn(s"URI $uri is invalid because ${e.getMessage}", e)
        
        Iterable.empty
    }
  }
  
  private[googlecloud] def put(key: URI, value: String): Unit = {
    val blobName = key.getPathWithoutLeadingSlash
    val blobId = BlobId.of(bucketName(key), blobName)
    val blobInfo = BlobInfo.newBuilder(blobId).setContentType("text/plain").build
   
    storage.create(blobInfo, value.getBytes("UTF8"))
  }
  
  private[googlecloud] def deleteWithPrefix(bucketName: String, prefix: String): Unit = {
    info(s"Deleting prefix '${prefix}'")
    
    val withPrefix = BlobListOption.prefix(prefix)

    def isDirectory(b: Blob): Boolean = b.getName.endsWith("/") 
    
    val listResult = storage.list(bucketName, withPrefix).getValues.asScala 

    val (dirs, nonDirs) = listResult.partition(isDirectory)

    if(nonDirs.nonEmpty) {
      for {
        item <- nonDirs
      } {
        info(s"Deleting '${item.getName}'")
        
        storage.delete(bucketName, item.getName)
      }
    }
    
    for {
      dir <- dirs.filterNot(_.getName == prefix)
    } {
      info(s"Deleting dir '${dir.getName}'")
      
      deleteWithPrefix(bucketName, dir.getName)
    }
  }
}

object GcsCloudStorageDriver {
  def fromConfig(config: GoogleCloudConfig): Try[GcsCloudStorageDriver] = fromCredentialsFile(config.credentialsFile)

  def fromCredentialsFile(credentialsFile: Path): Try[GcsCloudStorageDriver] = {
    if (Files.exists(credentialsFile)) {
      Success(GcsCloudStorageDriver(credentialsFile))
    } else {
      Tries.failure(s"Google Cloud credential not found at $credentialsFile")
    }
  }
}

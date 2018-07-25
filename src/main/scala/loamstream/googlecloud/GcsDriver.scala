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

/**
 * @author kyuksel
 *         date: 3/5/17
 */
final case class GcsDriver(credentialsFile: Path) extends CloudStorageDriver with Loggable {
  /**
   * Instantiate a GCS handle using given credentials.
   * If no credentials provided, attempt to find credentials
   * that might be set in the environment.
   */
  private lazy val storage: Storage =
    StorageOptions.newBuilder
      .setCredentials(ServiceAccountCredentials.fromStream(java.nio.file.Files.newInputStream(credentialsFile)))
      .build
      .getService

  // The name for the bucket
  private[googlecloud] def bucketName(uri: URI): String = uri.getHost

  /**
   * Return the object (blob) 'uri' identifies.
   * If 'uri' is a directory, return the list of all objects (blobs)
   * underneath it (directly or recursively).
   */
  def blobsAt(uri: URI): Iterable[BlobMetadata] = {
    import loamstream.util.Uris.Implicits._
    import scala.collection.JavaConverters._

    try {
      val withPrefix = BlobListOption.prefix(uri.getPathWithoutLeadingSlash)
      val withRelevantFields = BlobListOption.fields(
        BlobField.NAME,
        BlobField.UPDATED,
        BlobField.MD5HASH)

      storage.list(bucketName(uri), withPrefix, withRelevantFields).getValues.asScala
        .map(b => BlobMetadata(b.getName, b.getMd5, b.getUpdateTime))
    } catch {
      case e: StorageException =>
        warn(s"URI $uri is invalid because ${e.getMessage.toLowerCase()}")
        
        Iterable.empty
    }
  }
}

object GcsDriver {
  def fromConfig(config: GoogleCloudConfig): Try[GcsDriver] = fromCredentialsFile(config.credentialsFile)

  def fromCredentialsFile(credentialsFile: Path): Try[GcsDriver] = {
    if (Files.exists(credentialsFile)) {
      Success(GcsDriver(credentialsFile))
    } else {
      Tries.failure(s"Google Cloud credential not found at $credentialsFile")
    }
  }
}

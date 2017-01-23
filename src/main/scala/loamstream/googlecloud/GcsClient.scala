package loamstream.googlecloud

import java.net.URI
import java.nio.file.{Files, Path}
import java.time.Instant

import com.google.auth.oauth2.ServiceAccountCredentials
import com.google.cloud.storage.{Blob, Storage, StorageOptions}
import loamstream.util.HashType.Md5
import loamstream.util.{Hash, HashType, Tries}

import scala.util.{Success, Try}

/**
 * @author kyuksel
 *         date: Dec 6, 2016
 *
 * Wrapper around Google Cloud Storage JAVA API to expose methods for job recording purposes
 */
final case class GcsClient private[googlecloud] (credentialsFile: Path) extends CloudStorageClient {
  import loamstream.util.UriEnrichments._

  override val hashAlgorithm: HashType = Md5

  // Encapsulated MD5 hash of the storage object
  override def hash(uri: URI): Option[Hash] = Hash.fromStrings(hashStr(uri), hashAlgorithm.algorithmName)

  // If the storage object exists
  override def isPresent(uri: URI): Boolean = obj(uri).isDefined

  // Last update time of the object
  override def lastModified(uri: URI): Option[Instant] = obj(uri).map(o => Instant.ofEpochMilli(o.getUpdateTime))

  // Instantiate a GCS handle using given credentials.
  // If no credentials provided, attempt to find credentials that might be set in the environment
  private lazy val storage: Storage =
    StorageOptions.newBuilder
      .setCredentials(ServiceAccountCredentials.fromStream(java.nio.file.Files.newInputStream(credentialsFile)))
      .build
      .getService

  // The name for the bucket
  private[googlecloud] def bucketName(uri: URI): String = uri.getHost

  // Path for the storage object
  private[googlecloud] def objPath(uri: URI): String = uri.getPathWithoutLeadingSlash

  // Storage object option
  private[googlecloud] def obj(uri: URI): Option[Blob] = Option(storage.get(bucketName(uri), objPath(uri)))

  // MD5 hash of the storage object as String
  private[googlecloud] def hashStr(uri: URI): Option[String] = {
    obj(uri).map(_.getMd5)
  }
}

object GcsClient {
  def fromConfig(config: GoogleCloudConfig): Try[GcsClient] = fromCredentialsFile(config.credentialsFile)

  def fromCredentialsFile(credentialsFile: Path): Try[GcsClient] = {
    if (Files.exists(credentialsFile)) {
      Success(new GcsClient(credentialsFile))
    } else {
      Tries.failure(s"Google Cloud credential not found at $credentialsFile")
    }
  }
}

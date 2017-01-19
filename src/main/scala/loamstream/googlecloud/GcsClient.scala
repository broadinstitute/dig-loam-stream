package loamstream.googlecloud

import java.net.URI
import java.nio.file.{Path, Paths}
import java.time.Instant

import com.google.auth.oauth2.ServiceAccountCredentials
import com.google.cloud.storage.{Blob, Storage, StorageOptions}
import loamstream.util.HashType.Md5
import loamstream.util.Hash

/**
 * @author kyuksel
 *         date: Dec 6, 2016
 *
 * Wrapper around Google Cloud Storage JAVA API to expose methods for job recording purposes
 */
final case class GcsClient private[googlecloud] (credential: Path) extends CloudStorageClient {
  import GcsClient._

  // Encapsulated MD5 hash of the storage object
  override def hash(uri: URI): Option[Hash] = Hash.fromStrings(hashStr(uri), Md5.algorithmName).toOption

  // If the storage object exists
  override def isPresent(uri: URI): Boolean = obj(uri).isDefined

  // Last update time of the object
  override def lastModified(uri: URI): Option[Instant] =
    if (isPresent(uri)) { obj(uri).map(o => Instant.ofEpochMilli(o.getUpdateTime)) }
    else { None }
}

object GcsClient {
  import loamstream.util.UriEnrichments._

  private val credential = Paths.get("/Users/kyuksel/google_credential.json")

  def get: GcsClient = new GcsClient(credential)

  // Instantiate a GCS handle using given credentials.
  // If no credentials provided, attempt to find credentials that might be set in the environment
  private[googlecloud] lazy val storage: Storage =
    StorageOptions.newBuilder
      .setCredentials(ServiceAccountCredentials.fromStream(java.nio.file.Files.newInputStream(credential)))
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

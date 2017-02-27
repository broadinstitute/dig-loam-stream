package loamstream.googlecloud

import java.net.URI
import java.nio.file.{Files, Path}
import java.security.MessageDigest
import java.time.Instant
import javax.xml.bind.DatatypeConverter

import com.google.auth.oauth2.ServiceAccountCredentials
import com.google.cloud.storage.Storage.{BlobField, BlobListOption}
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

  // Encapsulated MD5 hash of the storage object/directory
  override def hash(uri: URI): Option[Hash] = {
    val msgDigest = MessageDigest.getInstance(hashAlgorithm.algorithmName)

    for {
        blobs <- blobsOpt(uri)
        hash <- blobs.map(_.getMd5)
      } msgDigest.update(DatatypeConverter.parseBase64Binary(hash))

    Option(Hash(msgDigest.digest, hashAlgorithm))
  }

  // If the storage object/directory exists
  override def isPresent(uri: URI): Boolean = blobsOpt(uri).isDefined

  // Last update time of the object
  // If 'uri' points to a directory, last update time of the most recently modified object within that directory
  override def lastModified(uri: URI): Option[Instant] = {
    blobsOpt(uri) match {
      case Some(blobs) => Some(Instant.ofEpochMilli(blobs.map(_.getUpdateTime).max))
      case _ =>  None
    }
  }

  // Instantiate a GCS handle using given credentials.
  // If no credentials provided, attempt to find credentials that might be set in the environment
  private lazy val storage: Storage =
    StorageOptions.newBuilder
      .setCredentials(ServiceAccountCredentials.fromStream(java.nio.file.Files.newInputStream(credentialsFile)))
      .build
      .getService

  // The name for the bucket
  private[googlecloud] def bucketName(uri: URI): String = uri.getHost

  // If 'uri' is a directory, (optionally) return the list of all objects underneath it (directly or recursively)
  // Else (optionally) return 'uri'
  private[googlecloud] def blobsOpt(uri:URI): Option[Iterable[Blob]] = {
    import scala.collection.JavaConverters._

    val withPrefix = BlobListOption.prefix(uri.getPathWithoutLeadingSlash)
    val withRelevantFields = BlobListOption.fields(BlobField.NAME, BlobField.UPDATED, BlobField.MD5HASH)

    Option(
      storage.list(bucketName(uri), withPrefix, withRelevantFields)
        .getValues.asScala
        .filterNot(_.getName.endsWith("/")) // eliminate directories
    )
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

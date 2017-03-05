package loamstream.googlecloud

import java.net.URI
import java.nio.file.{Files, Path}
import java.security.MessageDigest
import java.time.Instant
import javax.xml.bind.DatatypeConverter

import com.google.cloud.storage.Storage.{BlobField, BlobListOption}
import com.google.cloud.storage.{Blob, StorageException}
import loamstream.util.HashType.Md5
import loamstream.util.{Hash, HashType, Loggable, Tries}

import scala.util.{Success, Try}

/**
 * @author kyuksel
 *         date: Dec 6, 2016
 *
 * Wrapper around Google Cloud Storage JAVA API to expose methods for job recording purposes
 */
final case class GcsClient(driver: CloudStorageDriver) extends CloudStorageClient with Loggable {
  import loamstream.util.UriEnrichments._

  override val hashAlgorithm: HashType = Md5

  // Encapsulated MD5 hash of the storage object/directory
  override def hash(uri: URI): Option[Hash] = {
    val bs = blobs(uri)

    if (bs.isEmpty) { None }
    else {
      val hashes = bs.map(_.hash).toArray.sorted.mkString
      val binaryHashes = DatatypeConverter.parseBase64Binary(hashes)

      val msgDigest = MessageDigest.getInstance(hashAlgorithm.algorithmName)
      msgDigest.update(binaryHashes)

      val digest = msgDigest.digest

      Option(Hash(digest, hashAlgorithm))
    }
  }

  // If the storage object/directory exists
  override def isPresent(uri: URI): Boolean = blobs(uri).nonEmpty

  // Last update time of the object
  // If 'uri' points to a directory, last update time of the most recently modified object within that directory
  override def lastModified(uri: URI): Option[Instant] = {
    Try(Instant.ofEpochMilli(blobs(uri).map(_.updateTime).max)).toOption
  }

  private[googlecloud] def isDirectory(blob: BlobMetadata): Boolean = blob.name.endsWith("/")

  // Useful to distinguish, for instance, `x.gz` from `x.gz.tbi`
  private[googlecloud] def matchesSegment(blob: BlobMetadata, segment: String): Boolean = {
    blob.name.split("/").contains(segment)
  }

  private[googlecloud] def blobs(uri:URI): Iterable[BlobMetadata] = {
    driver.blobsAt(uri)
      .filterNot(isDirectory)
      .filter(matchesSegment(_, uri.lastSegment))
  }
}

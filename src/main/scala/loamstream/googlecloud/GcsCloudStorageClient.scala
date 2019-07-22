package loamstream.googlecloud

import java.net.URI
import java.time.Instant
import javax.xml.bind.DatatypeConverter

import loamstream.util.HashType.Md5
import loamstream.util._

import scala.util.Try

/**
 * @author kyuksel
 *         date: Dec 6, 2016
 *
 * Wrapper around Google Cloud Storage JAVA API to expose methods for job recording purposes
 */
final case class GcsCloudStorageClient(driver: CloudStorageDriver) extends CloudStorageClient with Loggable {
  import loamstream.util.Uris.Implicits._

  override val hashAlgorithm: HashType = Md5

  // Encapsulated MD5 hash of the storage object/directory
  override def hash(uri: URI): Option[Hash] = {
    val bs = blobs(uri)

    bs.toSeq match {
      case Nil => None
      //For a single blob, use the hash computed by Google
      case Seq(only) => Hash.fromStrings(Some(only.hash), hashAlgorithm.algorithmName)
      //For multiple blobs, as will be the case for a 'directory', hash their (Google-supplied) hashes
      case _ => {
        val hashStrings: Iterator[String] = bs.map(_.hash).toArray.sorted.toIterator
        val hashBytes = hashStrings.map(DatatypeConverter.parseBase64Binary)

        Option(Hashes.digest(hashAlgorithm)(hashBytes))
      }
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
  private[googlecloud] def matchesSegment(segment: String)(blob: BlobMetadata): Boolean = {
    blob.name.split('/').contains(segment)
  }

  private[googlecloud] def blobs(uri: URI): Iterable[BlobMetadata] = {
    LazyIterable {
      driver.blobsAt(uri).iterator.filter(matchesSegment(uri.lastSegment))
    }
  }
}

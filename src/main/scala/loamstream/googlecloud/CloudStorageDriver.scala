package loamstream.googlecloud

import java.net.URI

/**
 * @author kyuksel
 *         date: 3/5/17
 */
trait CloudStorageDriver {
  def blobsAt(uri: URI): Iterable[BlobMetadata]
}

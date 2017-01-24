package loamstream.googlecloud

import java.net.URI
import java.time.Instant

import loamstream.util.{Hash, HashType}

/**
 * @author kyuksel
 * Jan 13, 2017
 */
trait CloudStorageClient {
  def hashAlgorithm: HashType

  def hash(uri: URI): Option[Hash]
  
  def isPresent(uri: URI): Boolean
  
  def lastModified(uri: URI): Option[Instant]
}
package loamstream.model.jobs

import java.net.URI
import java.nio.file.Files
import java.nio.file.Path
import java.time.Instant

import loamstream.googlecloud.CloudStorageClient
import loamstream.util.Hash
import loamstream.util.HashType
import loamstream.util.Hashes
import loamstream.util.Paths
import loamstream.util.Paths.normalizePath

/**
 * @author clint
 * @author kaan
 * Aug 1, 2016
 *
 * A trait representing a handle to the output of a job; for now, we're primarily concerned with the case where
 * that output is a file, directory, or Google Cloud Storage objects but other output types are possible.
 */
sealed trait DataHandle {
  def isPresent: Boolean
  
  final def isMissing: Boolean = !isPresent
  
  def hash: Option[Hash]

  final def hashType: Option[HashType] = hash.map(_.tpe)

  def lastModified: Option[Instant]

  def location: String

  def toStoreRecord: StoreRecord
}

object DataHandle {
  /**
   * A handle to an output of a job stored at 'path'.  Hashes and modification times are re-computed on
   * each access.
   */
  final case class PathHandle private (path: Path) extends DataHandle {
    
    override def isPresent: Boolean = Files.exists(path)

    override def hash: Option[Hash] = if (isPresent) Option(Hashes.sha1(path)) else None

    override def lastModified: Option[Instant] = {
      if (isPresent) Option(Paths.lastModifiedTime(path)) else None
    }

    override def location: String = Paths.normalize(path)

    override def toStoreRecord: StoreRecord = {
      StoreRecord( 
          loc = location,
          isPresent = isPresent,
          hash = hashToString(hash),
          hashType = hashTypeToString(hashType),
          lastModified = lastModified)
    }

    def normalized: PathHandle = this
  }
  
  object PathHandle {
    def apply(path: Path): PathHandle = new PathHandle(normalizePath(path))
  }

  final case class GcsUriHandle(uri: URI, client: Option[CloudStorageClient]) extends DataHandle {
    override def isPresent = client.exists(_.isPresent(uri))

    override def hash: Option[Hash] = client.flatMap(_.hash(uri))

    override def lastModified: Option[Instant] = client.flatMap(_.lastModified(uri))

    override def location: String = uri.toString

    override def toStoreRecord: StoreRecord = {
      StoreRecord( 
          loc = location,
          isPresent = isPresent,
          hash = hashToString(hash),
          hashType = hashTypeToString(hashType),
          lastModified = lastModified)
    }
  }
  
  private def hashToString(hashOpt: Option[Hash]): Option[String] = hashOpt.map(_.valueAsBase64String)
  
  private def hashTypeToString(hashOpt: Option[HashType]): Option[String] = hashOpt.map(_.algorithmName)
}

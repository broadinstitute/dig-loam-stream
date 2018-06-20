package loamstream.model.jobs

import java.net.URI
import java.nio.file.Files
import java.nio.file.Path
import java.time.Instant

import loamstream.googlecloud.CloudStorageClient
import loamstream.util.Hash
import loamstream.util.HashType
import loamstream.util.Hashes
import loamstream.util.PathUtils
import loamstream.util.PathUtils.normalizePath
import loamstream.model.execute.Locations

/**
 * @author clint
 * @author kaan
 * Aug 1, 2016
 *
 * A trait representing a handle to the output of a job; for now, we're primarily concerned with the case where
 * that output is a file, directory, or Google Cloud Storage objects but other output types are possible.
 */
sealed trait Output {
  def isPresent: Boolean
  
  final def isMissing: Boolean = !isPresent
  
  def hash: Option[Hash]

  final def hashType: Option[HashType] = hash.map(_.tpe)

  def lastModified: Option[Instant]

  def location: String

  def toOutputRecord: OutputRecord
}

object Output {
  /**
   * A handle to an output of a job stored at 'path'.  Hashes and modification times are re-computed on
   * each access.
   */
  final case class PathOutput private (
      path: Path, 
      locations: Locations[Path]/* = Locations.identity*/) extends Output {
    
    //TODO
    lazy val pathInHost: Path = locations.inHost(path)
    lazy val pathInContainer: Path = locations.inContainer(path)
    
    override def isPresent: Boolean = Files.exists(pathInHost)

    override def hash: Option[Hash] = if (isPresent) Option(Hashes.sha1(pathInHost)) else None

    override def lastModified: Option[Instant] = {
      if (isPresent) Option(PathUtils.lastModifiedTime(pathInHost)) else None
    }

    override def location: String = PathUtils.normalize(pathInHost)

    override def toOutputRecord: OutputRecord = {
      OutputRecord( 
          loc = location,
          isPresent = isPresent,
          hash = hashToString(hash),
          hashType = hashTypeToString(hashType),
          lastModified = lastModified)
    }

    def normalized: PathOutput = this
  }
  
  object PathOutput {
    def apply(path: Path, locations: Locations[Path]): PathOutput = new PathOutput(normalizePath(path), locations)
  }

  final case class GcsUriOutput(uri: URI, client: Option[CloudStorageClient]) extends Output {
    override def isPresent = client.exists(_.isPresent(uri))

    override def hash: Option[Hash] = client.flatMap(_.hash(uri))

    override def lastModified: Option[Instant] = client.flatMap(_.lastModified(uri))

    override def location: String = uri.toString

    override def toOutputRecord: OutputRecord = {
      OutputRecord( 
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

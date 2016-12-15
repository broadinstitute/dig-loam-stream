package loamstream.model.jobs

import java.nio.file.Files
import java.nio.file.Path

import loamstream.util.{Hash, HashType, Hashes, PathUtils}
import java.time.Instant
import java.nio.file.Paths

/**
 * @author clint
 *         kaan
 * date: Aug 1, 2016
 * 
 * A trait representing a handle to the output of a job; for now, we're primarily concerned with the case where
 * that output is a file or directory, but other output types are possible.
 */
trait Output {
  def isPresent: Boolean
  
  final def isMissing: Boolean = !isPresent
  
  def hash: Hash

  def hashType: HashType

  def lastModified: Instant

  def location: String

  def toOutputRecord: OutputRecord
}

object Output {
  private def normalize(p: Path): Path = Paths.get(PathUtils.normalize(p))
  
  /**
   * A handle to an output of a job stored at 'path'.  Hashes and modification times are re-computed on
   * each access.
   */
  final case class PathOutput(path: Path) extends PathBased {
    override def hash: Hash = Hashes.sha1(path)

    override def hashType: HashType = HashType.Sha1

    override def normalized: PathBased = copy(path = normalize(path))
    
    override def lastModified: Instant = {
      if(isPresent) PathUtils.lastModifiedTime(path) else Instant.ofEpochMilli(0) 
    }

    override def toOutputRecord: OutputRecord = {
      OutputRecord(location, Option(hash.valueAsHexString), Option(lastModified))
    }
  }

  // TODO: Remove and have PathOutput directly subclass Output
  sealed trait PathBased extends Output {
    def path: Path
    
    def normalized: PathBased
    
    final override def isPresent: Boolean = Files.exists(path)

    final override def location: String = PathUtils.normalize(path)

    final def toPathOutput: PathOutput = this match {
      case po: PathOutput => po
      case _ => PathOutput(path)
    }
  }
  
  object PathBased {
    /**
     * Allow pattern matching on and extracting paths from path-based Outputs. 
     */
    def unapply(output: Output): Option[Path] = output match {
      case pb: PathBased => Some(pb.path)
      case _ => None
    }
  }
}

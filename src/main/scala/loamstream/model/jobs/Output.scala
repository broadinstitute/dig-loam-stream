package loamstream.model.jobs

import java.nio.file.Files
import java.nio.file.Path
import loamstream.util.Hash
import loamstream.util.Hashes
import java.time.Instant
import loamstream.util.PathUtils
import java.nio.file.Paths

/**
 * @author clint
 * date: Aug 1, 2016
 * 
 * A trait representing a handle to the output of a job; for now, we're primarily concerned with the case where
 * that output is a file or directory, but other output types are possible.
 */
trait Output {
  def isPresent: Boolean
  
  final def isMissing: Boolean = !isPresent
  
  def hash: Hash
  
  def lastModified: Instant
}

object Output {
  import PathUtils.normalizePath
  
  /**
   * A handle to an output of a job stored at 'path'.  Hashes and modification times are re-computed on
   * each access.
   */
  final case class PathOutput(path: Path) extends PathBased {
    override def hash: Hash = Hashes.sha1(path)
    
    override def normalized: PathBased = copy(path = normalizePath(path))
    
    override def lastModified: Instant = {
      if(isPresent) PathUtils.lastModifiedTime(path) else Instant.ofEpochMilli(0) 
    }
  }
  
  /**
   * A handle to cached data about a path-based output.
   */
  final case class CachedOutput(path: Path, hash: Hash, lastModified: Instant) extends PathBased {
    override def normalized: PathBased = copy(path = normalizePath(path))
  }
  
  sealed trait PathBased extends Output {
    def path: Path
    
    def normalized: PathBased
    
    final override def isPresent: Boolean = Files.exists(path)
    
    final def toCachedOutput: CachedOutput = this match {
      case co: CachedOutput => co
      case _ => CachedOutput(path, hash, lastModified)
    }
    
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
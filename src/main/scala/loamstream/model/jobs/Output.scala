package loamstream.model.jobs

import java.io.File
import java.nio.file.Files
import java.nio.file.Path
import loamstream.util.Hash
import loamstream.util.Hashes
import java.time.Instant
import loamstream.util.PathUtils

/**
 * @author clint
 * date: Aug 1, 2016
 */
trait Output {
  def isPresent: Boolean
  
  final def isMissing: Boolean = !isPresent
  
  def hash: Hash
  
  def lastModified: Instant
}

object Output {
  final case class PathOutput(path: Path) extends PathBased {
    override def hash: Hash = Hashes.sha1(path)
    
    override def lastModified: Instant = {
      if(isPresent) PathUtils.lastModifiedTime(path) else Instant.ofEpochMilli(0) 
    }
  }
  
  final case class CachedOutput(path: Path, hash: Hash, lastModified: Instant) extends PathBased
  
  trait PathBased extends Output {
    def path: Path
    
    final override def isPresent: Boolean = Files.exists(path)
    
    override def hashCode: Int = path.hashCode
    
    override def equals(other: Any): Boolean = other match {
      case that: PathBased => this.path.toAbsolutePath == that.path.toAbsolutePath
      case _ => false
    }
  }
}
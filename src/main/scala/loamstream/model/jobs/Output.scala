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
  
  def hash: Hash
  
  def lastModified: Instant
}

object Output {
  final case class PathOutput(file: Path) extends Output {
    override def isPresent: Boolean = Files.exists(file)
    
    override def hash: Hash = Hashes.sha1(file)
    
    override def lastModified: Instant = {
      if(isPresent) PathUtils.lastModifiedTime(file) else Instant.ofEpochMilli(0) 
    }
  }
}
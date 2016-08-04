package loamstream.model.jobs

import java.io.File
import java.nio.file.Files
import java.nio.file.Path
import loamstream.util.Hash
import loamstream.util.Hashes

/**
 * @author clint
 * date: Aug 1, 2016
 */
trait Output {
  def isPresent: Boolean
  
  def hash: Hash
}

object Output {
  final case class FileOutput(file: Path) extends Output {
    override def isPresent: Boolean = Files.exists(file)
    
    override def hash: Hash = Hashes.sha1(file)
  }
}
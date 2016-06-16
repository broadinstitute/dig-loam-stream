package loamstream.util

import java.io.File
import java.io.FileNotFoundException

import java.nio.file.Path
import java.nio.file.Paths

import scala.util.Failure
import scala.util.Success
import scala.util.Try

/**
 * @author clint
 * date: Jun 15, 2016
 */
object Files {
  private[util] val tempFilePrefix = "loamstream"
  
  def tempFile(suffix: String): Path = {
    File.createTempFile(tempFilePrefix, suffix).toPath.toAbsolutePath
  }
  
  def tryFile(fileName: String): Try[Path] = tryFile(Paths.get(fileName))
  
  def tryFile(path: Path): Try[Path] = {
    if(path.toFile.exists) { Success(path) }
    else { Failure(new FileNotFoundException(s"Can't find '$path'")) }
  }
}
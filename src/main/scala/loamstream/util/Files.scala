package loamstream.util

import java.io.File
import java.io.FileNotFoundException
import java.io.FileWriter
import java.nio.file.Path
import java.nio.file.Paths

import scala.util.Failure
import scala.util.Success
import scala.util.Try
import java.util.stream.Collectors

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
  
  def writeTo(file: Path)(contents: String): Unit = {
    LoamFileUtils.enclosed(new FileWriter(file.toFile)) { 
      _.write(contents)
    }
  }
  
  def readFrom(file: Path): String = {
    import java.io._
      
    LoamFileUtils.enclosed(new BufferedReader(new FileReader(file.toFile))) { reader =>
      import scala.collection.JavaConverters._
        
      reader.lines.collect(Collectors.toList()).asScala.mkString(System.lineSeparator)
    }
  }
}
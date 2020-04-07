package loamstream.loam.intake.flip

import java.nio.file.Path
import java.util.UUID

import org.apache.commons.io.FileUtils

import loamstream.TestHelpers
import loamstream.util.Files
import loamstream.util.CanBeClosed
import java.util.zip.GZIPOutputStream
import java.io.FileOutputStream
import java.io.OutputStreamWriter
import java.io.Writer

/**
 * @author clint
 * Apr 1, 2020
 */
object Helpers {
  def withZippedAndUnzippedTestFiles[A](contents: String)(body: Path => A): A = {
    withTestFile(contents)(body)
    
    withGzippedTestFile(contents)(body)
  }
  
  def withTestFile[A](contents: String)(body: Path => A): A = {
    withFile(contents, "txt")(Files.writeTo(_)(contents))(body)
  }
  
  def withGzippedTestFile[A](contents: String)(body: Path => A): A = {
    def write(file: Path): Unit = {
      val writer = new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(file.toFile)))
        
      CanBeClosed.enclosed(writer)(_.write(contents))
    }
    
    withFile(contents, "gz")(write)(body)
  }
  
  private def withFile[A](contents: String, extension: String)(write: Path => Unit)(body: Path => A): A = {
    TestHelpers.withWorkDir(getClass.getSimpleName) { workDir =>
      val file = workDir.resolve(s"${UUID.randomUUID.toString}.${extension}")
      
      try {
        write(file)
        
        body(file)
      } finally {
        FileUtils.deleteQuietly(file.toFile)
      }
    }
  }
}

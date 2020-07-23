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
  def withTestFile[A](contents: String)(body: Path => A): A = {
    withFile(contents, "txt")(Files.writeTo(_)(contents))(body)
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

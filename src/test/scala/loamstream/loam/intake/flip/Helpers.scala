package loamstream.loam.intake.flip

import java.nio.file.Path
import java.util.UUID

import org.apache.commons.io.FileUtils

import loamstream.TestHelpers
import loamstream.util.Files

/**
 * @author clint
 * Apr 1, 2020
 */
object Helpers {
  def withTestFile[A](contents: String)(body: Path => A): A = {
    TestHelpers.withWorkDir(getClass.getSimpleName) { workDir =>
      val file = workDir.resolve(UUID.randomUUID.toString)
      
      try {
        Files.writeTo(file)(contents)
        
        body(file)
      } finally {
        FileUtils.deleteQuietly(file.toFile)
      }
    }
  }
}

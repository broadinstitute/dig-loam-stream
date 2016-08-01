package loamstream.util

import java.io.{File, FileNotFoundException, FileWriter}
import java.nio.file.Path
import java.nio.file.Paths

import scala.util.Failure
import scala.util.Success
import scala.util.Try
import java.util.stream.Collectors

/**
 * @author clint
 *         date: Jun 15, 2016
 */
object Files {
  private[util] val tempFilePrefix = "loamstream"

  /**
   * Creates an empty file in the *default temporary-file* directory, using
   * the given prefix and suffix to generate its name.
   */
  def tempFile(suffix: String): Path = {
    File.createTempFile(tempFilePrefix, suffix).toPath.toAbsolutePath
  }

  /**
   * Creates an empty file in the *specified* directory, using
   * the given prefix and suffix to generate its name.
   */
  def tempFile(suffix: String, directory: File): Path = {
    File.createTempFile(tempFilePrefix, suffix, directory).toPath.toAbsolutePath
  }

  def tryFile(fileName: String): Try[Path] = tryFile(Paths.get(fileName))

  def tryFile(path: Path): Try[Path] = {
    if (path.toFile.exists) {
      Success(path)
    } else {
      Failure(new FileNotFoundException(s"Can't find '$path'"))
    }
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

  def readFromAsUtf8(file: Path): String = StringUtils.fromUtf8Bytes(java.nio.file.Files.readAllBytes(file))
}

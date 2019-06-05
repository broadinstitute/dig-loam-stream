package loamstream.util

import java.io.BufferedReader
import java.io.BufferedWriter
import java.io.File
import java.io.FileInputStream
import java.io.FileNotFoundException
import java.io.FileOutputStream
import java.io.InputStreamReader
import java.io.OutputStreamWriter
import java.io.Reader
import java.io.Writer
import java.nio.charset.StandardCharsets
import java.nio.file.{ Files => JFiles }
import java.nio.file.Path
import java.nio.file.{ Paths => JPaths }
import java.util.stream.Collectors
import java.util.zip.GZIPInputStream
import java.util.zip.GZIPOutputStream

import scala.io.Codec
import scala.io.Source
import scala.util.Failure
import scala.util.Success
import scala.util.Try
import java.nio.file.StandardCopyOption

/**
  * @author clint
  *         date: Jun 15, 2016
  */
object Files {
  private[util] val tempFilePrefix = "loamstream"

  /**
   * Copy src to dest, overwriting dest if it exists   
   */
  def copyAndOverwrite(src: Path, dest: Path): Unit = JFiles.copy(src, dest, StandardCopyOption.REPLACE_EXISTING)
  
  /**
    * Creates an empty file in the *default temporary-file* directory, using
    * the given prefix and suffix to generate its name.
    */
  def tempFile(suffix: String): Path = File.createTempFile(tempFilePrefix, suffix).toPath.toAbsolutePath

  /**
    * Creates an empty file in the *specified* directory, using
    * the given prefix and suffix to generate its name.
    */
  def tempFile(suffix: String, directory: File): Path = {
    require(directory.isDirectory, s"'$directory' must be a directory")
    require(directory.exists, s"'$directory' must exist")

    File.createTempFile(tempFilePrefix, suffix, directory).toPath.toAbsolutePath
  }

  /**
    * Creates the specified directory if it doesn't exist, including any
    * necessary but nonexistent parent directories.  Note that if this
    * operation fails it may have succeeded in creating some of the necessary
    * parent directories.
    */
  def createDirsIfNecessary(directory: Path): Unit = {
    val dir = directory.toFile

    if (! dir.exists) { dir.mkdirs() }

    require(dir.exists)
  }

  /**
   * @param directory Directory to search in
   * @param regex Assumed to be a valid regular expression
   *
   * @return List of files that match specified regex within
   * the specified directory
   */
  def listFiles(directory: Path, regex: String): Iterable[File] = {
    val dir = directory.toFile

    if (dir.exists) {
      dir.listFiles.filter(_.getName.matches(regex))
    } else {
      Iterable.empty
    }
  }

  def tryFile(fileName: String): Try[Path] = tryFile(JPaths.get(fileName))

  def tryFile(path: Path): Try[Path] = {
    if (path.toFile.exists) {
      Success(path)
    } else {
      Failure(new FileNotFoundException(s"Can't find '$path'"))
    }
  }

  def writeTo(file: Path)(contents: String): Path = {
    doWriteTo(JFiles.newBufferedWriter(file, StandardCharsets.UTF_8), contents)
    
    file
  }

  def readFrom(file: Path): String = readFromAsUtf8(file)

  def readFrom(file: String): String = readFrom(JPaths.get(file))

  /** Writes to gzipped file */
  def writeToGzipped(file: Path)(contents: String): Unit = {
    val gZIPOutputStream = new GZIPOutputStream(new FileOutputStream(file.toFile))
    doWriteTo(new OutputStreamWriter(gZIPOutputStream, StandardCharsets.UTF_8), contents)
  }

  /** Read from gzipped file */
  def readFromGzipped(file: Path): String = {
    val gZIPInputStream = new GZIPInputStream(new FileInputStream(file.toFile))
    doReadFrom(new InputStreamReader(gZIPInputStream, StandardCharsets.UTF_8))
  }

  def readFromAsUtf8(file: Path): String = StringUtils.fromUtf8Bytes(java.nio.file.Files.readAllBytes(file))

  private def doWriteTo(writer: Writer, contents: String): Unit = {
    CanBeClosed.enclosed(writer)(_.write(contents))
  }

  private def doReadFrom(reader: Reader): String = {
    def toBufferedReader = reader match {
      case br: BufferedReader => br
      case _ => new BufferedReader(reader)
    }

    CanBeClosed.enclosed(toBufferedReader) { bufferedReader =>
      import scala.collection.JavaConverters._

      bufferedReader.lines.collect(Collectors.toList()).asScala.mkString(System.lineSeparator)
    }
  }

  type LineFilter = String => Boolean

  object LineFilter {
    type Factory = () => LineFilter

    private val acceptsAllLineFilter: LineFilter = _ => true

    val acceptAll: Factory = () => acceptsAllLineFilter
  }

  //TODO: Currently, this always creates the output file, even if sourcePaths is empty.  Is that the right thing to do?
  def mergeLinesGzipped(
                         sourcePaths: Iterable[Path],
                         targetPath: Path,
                         lineFilterFactory: LineFilter.Factory = LineFilter.acceptAll): Unit = {

    val lineFilter = lineFilterFactory()

    val writer = {
      val gZIPOutputStream = new GZIPOutputStream(new FileOutputStream(targetPath.toFile))
      new BufferedWriter(new OutputStreamWriter(gZIPOutputStream, StandardCharsets.UTF_8))
    }

    def inputStreamFor(path: Path) = new GZIPInputStream(new FileInputStream(path.toFile))

    CanBeClosed.enclosed(writer) { writer =>
      for (sourcePath <- sourcePaths) {
        val source = Source.createBufferedSource(inputStreamFor(sourcePath))(Codec.UTF8)
        CanBeClosed.enclosed(source) { source =>
          //TODO: Use System.lineSeparator for platform-specific line endings, instead of '\n'?
          source.getLines.filter(lineFilter).map(line => s"$line\n").foreach(writer.write)
        }
      }
    }
  }

  def filterFile(inFile: Path, outFile: Path)(filter: String => Boolean): Unit = {
    CanBeClosed.enclosed(Source.fromFile(inFile.toFile)) { source =>
      CanBeClosed.enclosed(JFiles.newBufferedWriter(outFile, StandardCharsets.UTF_8)) { writer =>
        source.getLines.filter(filter).foreach { line =>
          writer.write(line)
          writer.newLine()
        }
      }
    }
  }

  def mapFile(inFile: Path, outFile: Path)(mapper: String => String): Unit = {
    CanBeClosed.enclosed(Source.fromFile(inFile.toFile)) { source =>
      CanBeClosed.enclosed(JFiles.newBufferedWriter(outFile, StandardCharsets.UTF_8)) { writer =>
        source.getLines.map(mapper).foreach { line =>
          writer.write(line)
          writer.newLine()
        }
      }
    }
  }

  private def withLines[A](file: Path)(f: Iterator[String] => A): A = {
    CanBeClosed.enclosed(Source.fromFile(file.toFile)) { source =>
      f(source.getLines)
    }
  }
  
  def countLines(file: Path): Long = withLines(file)(_.size)
  
  def getLines(file: Path): IndexedSeq[String] = withLines(file)(_.toIndexedSeq)
  
  def getNonEmptyLines(file: Path): IndexedSeq[String] = withLines(file)(_.filter(_.nonEmpty).toIndexedSeq)
}

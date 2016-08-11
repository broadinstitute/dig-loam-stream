package loamstream.util

import java.io._
import java.nio.file.Path
import java.nio.file.Paths

import scala.util.Failure
import scala.util.Success
import scala.util.Try
import java.util.stream.Collectors
import java.util.zip.{GZIPInputStream, GZIPOutputStream}

import scala.io.Source

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
    doWriteTo(new FileWriter(file.toFile), contents)
  }

  def readFrom(file: Path): String = {
    doReadFrom(new FileReader(file.toFile))
  }

  /** Writes to gzipped file */
  def writeToGzipped(file: Path)(contents: String): Unit = {
    doWriteTo(new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(file.toFile))), contents)
  }

  /** Read from gzipped file */
  def readFromGzipped(file: Path): String = {
    doReadFrom(new InputStreamReader(new GZIPInputStream(new FileInputStream(file.toFile))))
  }

  def readFromAsUtf8(file: Path): String = StringUtils.fromUtf8Bytes(java.nio.file.Files.readAllBytes(file))

  private def doWriteTo(writer: Writer, contents: String): Unit = {
    LoamFileUtils.enclosed(writer)(_.write(contents))
  }

  private def doReadFrom(reader: Reader): String = {
    def toBufferedReader = reader match {
      case br: BufferedReader => br
      case _ => new BufferedReader(reader)
    }

    LoamFileUtils.enclosed(toBufferedReader) { bufferedReader =>
      import scala.collection.JavaConverters._

      bufferedReader.lines.collect(Collectors.toList()).asScala.mkString(System.lineSeparator)
    }
  }

  type LineFilter = String => Boolean

  object LineFilter {
    type Factory = () => LineFilter
    
    private object AcceptsAllLineFilter extends LineFilter {
      override def apply(line: String): Boolean = true
    }
    
    val acceptAll: Factory = () => AcceptsAllLineFilter
    
    //TODO: It would be nice to not need a stateful predicate
    val onlyFirstVcfHeader: Factory = () => new LineFilter {
      private var firstVcfHeaderIsPast = false

      override def apply(line: String): Boolean = {
        val lineIsNotHeader = !line.startsWith("##")
        if (firstVcfHeaderIsPast) {
          lineIsNotHeader
        } else {
          if (lineIsNotHeader) {
            firstVcfHeaderIsPast = true
          }
          true
        }
      }
    }
  }

  //TODO: Currently, this always creates the output file, even if sourcePaths is empty.  Is that the right thing to do?
  def mergeLinesGzipped(
      sourcePaths: Iterable[Path], 
      targetPath: Path,
      lineFilterFactory: LineFilter.Factory = LineFilter.acceptAll): Unit = {
    
    val lineFilter = lineFilterFactory()
    
    val writer = {
      new BufferedWriter(new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(targetPath.toFile))))
    }
    
    def inputStreamFor(path: Path) = new GZIPInputStream(new FileInputStream(path.toFile))
    
    LoamFileUtils.enclosed(writer) { writer =>
      for (sourcePath <- sourcePaths) {
        val source = Source.createBufferedSource(inputStreamFor(sourcePath))
        LoamFileUtils.enclosed(source) { source =>
          //TODO: Use System.lineSeparator for platform-specific line endings, instead of '\n'?
          source.getLines.filter(lineFilter).map(line => s"$line\n").foreach(writer.write)
        }
      }
    }
  }
}

package loamstream.util

import org.scalatest.FunSuite
import scala.util.Success
import java.nio.file.Paths
import java.util.stream.Collectors
import java.nio.file.{Path, Files => JFiles}

/**
  * @author clint
  *         date: Jun 15, 2016
  */
final class FilesTest extends FunSuite {
  test("tempFile in default temporary-file directory") {
    val path = Files.tempFile("foo")

    assert(path.toString.endsWith("foo"))
    assert(path.getFileName.toString.startsWith(Files.tempFilePrefix))
    assert(path.toFile.exists)
  }

  test("tempFile in specified temporary-file directory") {
    val parentOfDefaultTempDir = Files.tempFile("foo").getParent.toFile
    val path = Files.tempFile("foo", parentOfDefaultTempDir)

    assert(path.toString.endsWith("foo"))
    assert(path.getFileName.toString.startsWith(Files.tempFilePrefix))
    assert(path.toFile.exists)
  }

  test("tryFile(String)") {
    assert(Files.tryFile("foo").isFailure)

    val tempFile = Files.tempFile("bar")

    val Success(path) = Files.tryFile(tempFile.toString)

    assert(path == tempFile)
  }

  test("tryFile(Path)") {
    assert(Files.tryFile(Paths.get("foo")).isFailure)

    val tempFile = Files.tempFile("bar")

    val Success(path) = Files.tryFile(tempFile)

    assert(path == tempFile)
  }

  test("writeTo()() and readFrom()") {
    doWriteReadTest(Files.readFrom)
  }

  test("readFromAsUtf8") {
    doWriteReadTest(Files.readFromAsUtf8)
  }

  private def doWriteReadTest(read: Path => String): Unit = {
    val tempFile = Files.tempFile("foo")

    val contents =
      """hello
      world
        blah
        yo  """

    assert(Files.readFromAsUtf8(tempFile) == "")

    Files.writeTo(tempFile)(contents)

    assert(read(tempFile) == contents)
  }

  test("Write to compressed file and read back") {
    val content = "Hello World!\n" * 100
    val path = Files.tempFile("txt")
    Files.writeToGzipped(path)(content)
    val contentReread = Files.readFromGzipped(path)
    assert(StringUtils.unwrapLines(contentReread).trim === StringUtils.unwrapLines(content).trim)
    assert(path.toFile.length() < 100)
  }

  test("Read from file compressed by an external tool") {
    val expected = "This is some text."

    val file = Paths.get("src/test/resources/foo.txt.gz")

    val actual = Files.readFromGzipped(file)

    assert(actual == expected)
  }
  test("Merging gzipped files") {
    val nFiles = 3
    val nLinesPerFile = 5
    val folder = JFiles.createTempDirectory("loamStreamFilesTest")
    val pathsAndContents = (0 until 3).map({ iFile =>
      val path = folder.resolve(s"file$iFile.txt")
      val content = (0 until nLinesPerFile).map(iLine => s"Line $iLine of file $iFile.").mkString("\n")
      (path, content)
    })
    pathsAndContents.foreach({ case (path, content) => Files.writeToGzipped(path)(content) })
    val paths = pathsAndContents.map({ case (path, _) => path })
    val pathOut = folder.resolve("fileOut.txt")
    Files.mergeLinesGzipped(paths, pathOut)
    val contents = pathsAndContents.map({ case (_, content) => content })
    val contentOutExpected = contents.mkString("\n")
    val contentOut = Files.readFromGzipped(pathOut)
    assert(contentOut === contentOutExpected)
  }
}
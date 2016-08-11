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
    val iFiles = 0 until nFiles
    val folder = JFiles.createTempDirectory("loamStreamFilesTest")
    val paths = iFiles.map(iFile => folder.resolve(s"file$iFile.txt"))
    
    val contents = iFiles.map { iFile =>
      (0 until nLinesPerFile).map(iLine => s"Line $iLine of file $iFile.").mkString("\n")
    }
    
    iFiles.foreach(iFile => Files.writeToGzipped(paths(iFile))(contents(iFile)))
    
    val pathOut = folder.resolve("fileOut.txt")
    Files.mergeLinesGzipped(paths, pathOut)
    val contentOutExpected = contents.mkString("\n")
    val contentOut = Files.readFromGzipped(pathOut)
    assert(contentOut === contentOutExpected)
  }
  
  test("Merging pseudo-VCF files, keeping only header of first file") {
    val nFiles = 3
    val nHeaderLines = 5
    val nBodyLines = 7
    val iFiles = 0 until nFiles
    val folder = JFiles.createTempDirectory("loamStreamFilesTest")
    val paths = iFiles.map(iFile => folder.resolve(s"file$iFile.vcf"))
    
    val headers = iFiles.map { iFile =>
      (0 until nHeaderLines).map(iLine => s"## Header line $iLine of file $iFile.").mkString("\n")
    }
    
    val bodies = iFiles.map { iFile =>
      (0 until nBodyLines).map(iLine => s"Body line $iLine of file $iFile.").mkString("\n")
    }
    
    val contents = iFiles.map(iFile => s"${headers(iFile)}\n${bodies(iFile)}")
    iFiles.foreach(iFile => Files.writeToGzipped(paths(iFile))(contents(iFile)))
    val pathOut = folder.resolve("fileOut.txt")
    Files.mergeLinesGzipped(paths, pathOut, Files.LineFilter.onlyFirstVcfHeader)
    val contentOutExpected = s"${headers(0)}\n${bodies.mkString("\n")}"
    val contentOut = Files.readFromGzipped(pathOut)
    assert(contentOut === contentOutExpected)
  }
  
  test("Merging empty bunch of pseudo-VCF files") {
    def doTest(filterFactory: Files.LineFilter.Factory): Unit = {
      val folder = JFiles.createTempDirectory("loamStreamFilesTest")
    
      val pathOut = folder.resolve("fileOut.txt")
    
      Files.mergeLinesGzipped(Nil, pathOut, filterFactory)
      
      assert(pathOut.toFile.exists)
      //File will contain gzip header, etc
      assert(pathOut.toFile.length != 0)
      //But the file should contain no actual data
      assert(Files.readFromGzipped(pathOut) == "")
    }
    
    doTest(Files.LineFilter.onlyFirstVcfHeader)
    
    doTest(Files.LineFilter.acceptAll)
  }
}
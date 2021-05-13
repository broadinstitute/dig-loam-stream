package loamstream.util

import java.nio.charset.StandardCharsets
import java.nio.file.{ Files => JFiles }
import java.nio.file.Path

import scala.collection.JavaConverters.asScalaIteratorConverter
import scala.util.Success

import org.scalatest.FunSuite

import loamstream.TestHelpers

import scala.collection.compat._

/**
  * @author clint
  *         date: Jun 15, 2016
  */
final class FilesTest extends FunSuite {
  import Paths.Implicits._
  import java.nio.file.Files.exists
  import loamstream.TestHelpers.path
  
  test("copyAndOverwrite") {
    TestHelpers.withWorkDir(getClass.getSimpleName) { workDir =>
      val foo = workDir / "foo"
      val bar = workDir / "bar"
      
      assert(exists(foo) === false)
      assert(exists(bar) === false)
      
      Files.writeTo(foo)("lalala")
      
      assert(exists(foo) === true)
      assert(exists(bar) === false)
      
      Files.copyAndOverwrite(foo, bar)
      
      assert(exists(foo) === true)
      assert(exists(bar) === true)
      
      assert(Files.readFrom(foo) === "lalala")
      assert(Files.readFrom(bar) === "lalala")
      
      Files.writeTo(foo)("asdf")
      
      assert(Files.readFrom(foo) === "asdf")
      assert(Files.readFrom(bar) === "lalala")
      
      Files.copyAndOverwrite(foo, bar)
      
      assert(Files.readFrom(foo) === "asdf")
      assert(Files.readFrom(bar) === "asdf")
      
      Files.copyAndOverwrite(foo, bar)
      
      assert(Files.readFrom(foo) === "asdf")
      assert(Files.readFrom(bar) === "asdf")
    }
  }
  
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

  test("createDirsIfNecessary") {
    val workDir = TestHelpers.getWorkDir(getClass.getSimpleName) 
    
    val dummy = workDir / "dummy1" / "dummy2"
    assert(!dummy.toFile.exists)

    Files.createDirsIfNecessary(dummy)
    assert(dummy.toFile.exists)

    assert(dummy.toFile.delete)
  }

  test("listFiles") {
    val workDir = TestHelpers.getWorkDir(getClass.getSimpleName)
    
    val dummy = (workDir / "dummy-listFiles").toAbsolutePath
    val dummyFileSuffix = ".dummy.txt"

    assert(!dummy.toFile.exists)
    assert(Files.listFiles(dummy, ".*") === List.empty)

    Files.createDirsIfNecessary(dummy)
    assert(Files.listFiles(dummy, ".*") === List.empty)

    val dummyFile1 = Files.tempFile(dummyFileSuffix, dummy.toFile).toFile
    assert(Files.listFiles(dummy, ".*") === List(dummyFile1))

    val dummyFile2 = Files.tempFile(dummyFileSuffix, dummy.toFile).toFile
    assert(Files.listFiles(dummy, ".*").to(Set) === Set(dummyFile1, dummyFile2))
    assert(Files.listFiles(dummy, ".*dummy.*").to(Set) === Set(dummyFile1, dummyFile2))
    assert(Files.listFiles(dummy, ".*duMMy.*").to(Set) === Set.empty)

    assert(dummyFile1.delete)
    assert(dummyFile2.delete)
    assert(dummy.toFile.delete)
  }

  test("tryFile(String)") {
    
    import TestHelpers.path
    import java.nio.file.Files.exists
    
    val nonexistentPath = "/foo/bar/baz/asdfasdasdasd"
    
    assert(exists(path(nonexistentPath)) === false)
    
    assert(Files.tryFile(nonexistentPath).isFailure)

    val tempFile = Files.tempFile("bar")
    
    assert(exists(tempFile))    

    val Success(p) = Files.tryFile(tempFile.toString)

    assert(p == tempFile)
  }

  test("tryFile(Path)") {
    import TestHelpers.path
    import java.nio.file.Files.exists
    
    val nonexistentPath = "/foo/bar/baz/asdfasdasdasd"
    
    assert(exists(path(nonexistentPath)) === false)
    
    assert(Files.tryFile(path(nonexistentPath)).isFailure)

    val tempFile = Files.tempFile("bar")
    
    assert(exists(tempFile))

    val Success(p) = Files.tryFile(tempFile)

    assert(p == tempFile)
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

    val file = path("src/test/resources/foo.txt.gz")

    val actual = Files.readFromGzipped(file)

    assert(actual == expected)
  }

  /** Asserts that both texts are the same except for possibly different line breaks */
  private def assertSameText(text1: String, text2: String): Unit = {
    assert(StringUtils.assimilateLineBreaks(text1) === StringUtils.assimilateLineBreaks(text2))
  }

  test("Merging gzipped files") {
    val nFiles = 3
    val nLinesPerFile = 5
    val iFiles = 0 until nFiles
    val folder = TestHelpers.getWorkDir("loamStreamFilesTest")
    val paths = iFiles.map(iFile => folder.resolve(s"file$iFile.txt"))

    val contents = iFiles.map { iFile =>
      (0 until nLinesPerFile).map(iLine => s"Line $iLine of file $iFile.").mkString("\n")
    }

    iFiles.foreach(iFile => Files.writeToGzipped(paths(iFile))(contents(iFile)))

    val pathOut = folder.resolve("fileOut.txt")
    Files.mergeLinesGzipped(paths, pathOut)
    val contentOutExpected = contents.mkString("\n")
    val contentOut = Files.readFromGzipped(pathOut)
    assertSameText(contentOut, contentOutExpected)
  }

  private val onlyFirstVcfHeader: Files.LineFilter.Factory = () => { 
    var firstVcfHeaderIsPast = false

    line => {
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
  
  test("Merging pseudo-VCF files, keeping only header of first file") {
    val nFiles = 3
    val nHeaderLines = 5
    val nBodyLines = 7
    val iFiles = 0 until nFiles
    val folder = TestHelpers.getWorkDir("loamStreamFilesTest")
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
    
    Files.mergeLinesGzipped(paths, pathOut, onlyFirstVcfHeader)
    
    val contentOutExpected = s"${headers(0)}\n${bodies.mkString("\n")}"
    val contentOut = Files.readFromGzipped(pathOut)
    
    assertSameText(contentOut, contentOutExpected)
  }

  test("Merging empty bunch of pseudo-VCF files") {
    def doTest(filterFactory: Files.LineFilter.Factory): Unit = {
      val folder = TestHelpers.getWorkDir("loamStreamFilesTest")

      val pathOut = folder.resolve("fileOut.txt")

      Files.mergeLinesGzipped(Nil, pathOut, filterFactory)

      assert(pathOut.toFile.exists)
      //File will contain gzip header, etc
      assert(pathOut.toFile.length != 0)
      //But the file should contain no actual data
      assert(Files.readFromGzipped(pathOut) == "")
    }

    doTest(onlyFirstVcfHeader)

    doTest(Files.LineFilter.acceptAll)
  }

  private def assertEachLine(file: Path, predicateDescription: String)(predicate: String => Boolean): Unit = {
    CanBeClosed.enclosed(JFiles.newBufferedReader(file, StandardCharsets.UTF_8)) { reader =>
      assert(reader.lines.iterator.asScala.forall(predicate), s"Not true for every line: $predicateDescription")
    }
  }

  test("countLines(...) and filterFile(...)") {
    val dir = JFiles.createTempDirectory("FilesTest")
    val inFile = dir.resolve("inFile.txt")
    val inFileContent =
      """
        |'Twas brillig, and the slithy toves
        |Did gyre and gimble in the wabe;
        |All mimsy were the borogoves,
        |And the mome raths outgrabe.
        |
        |"Beware the Jabberwock, my son!
        |The jaws that bite, the claws that catch!
        |Beware the Jubjub bird, and shun
        |The frumious Bandersnatch!"
        |
        |He took his vorpal sword in hand:
        |Long time the manxome foe he sought -
        |So rested he by the Tumtum tree,
        |And stood awhile in thought.
        |
        |And as in uffish thought he stood,
        |The Jabberwock, with eyes of flame,
        |Came whiffling through the tulgey wood,
        |And burbled as it came!
        |
        |One, two! One, two! and through and through
        |The vorpal blade went snicker-snack!
        |He left it dead, and with its head
        |He went galumphing back.
        |
        |"And hast thou slain the Jabberwock?
        |Come to my arms, my beamish boy!
        |O frabjous day! Callooh! Callay!"
        |He chortled in his joy.
        |
        |'Twas brillig, and the slithy toves
        |Did gyre and gimble in the wabe;
        |All mimsy were the borogoves,
        |And the mome raths outgrabe.
      """.stripMargin
    Files.writeTo(inFile)(inFileContent)
    val nLinesInFile = 36
    assert(Files.countLines(inFile) === nLinesInFile)
    for((template, nLinesWithTemplateExpected) <- Seq(("Jabberwock", 3), ("monkey", 0), ("e", 27))) {
      val outFile = dir.resolve(s"outFile$template.txt")
      Files.filterFile(inFile, outFile)(_.contains(template))
      assertEachLine(outFile, s"""contains "$template"""")(_.contains(template))
      val nLinesWithTemplate = Files.countLines(outFile)
      assert(nLinesWithTemplate === nLinesWithTemplateExpected,
        s"""Expected $nLinesWithTemplateExpected with "$template", but found $nLinesWithTemplate.""")
    }
  }
  
  private def withTempFile[A](f: Path => A): A = {
    val tempFile = Files.tempFile("foo")
    assert(tempFile.toFile.exists === true)
    
    try { f(tempFile) }
    finally {
      java.nio.file.Files.delete(tempFile)
      assert(tempFile.toFile.exists === false)
    }
  }
  
  test("getLines") {
    val contents = """|foo
                      |bar
                      |
                      |baz
                      |  
                      |  blerg  """.stripMargin
   
    withTempFile { tempFile =>                                            
      Files.writeTo(tempFile)(contents)
      
      assert(Files.readFromAsUtf8(tempFile) === contents)
      
      val lines = Files.getLines(tempFile)
      
      val expected = Seq("foo", "bar", "", "baz", "  ", "  blerg  ")
      
      assert(lines === expected)
    }
  }
  
  test("getNonEmptyLines") {
    val contents = """|
                      |foo
                      |bar
                      |
                      |
                      |baz
                      |  
                      |  blerg  
                      |
                      |""".stripMargin
   
    withTempFile { tempFile =>                                            
      Files.writeTo(tempFile)(contents)
      
      assert(Files.readFromAsUtf8(tempFile) === contents)
      
      val nonEmptyLines = Files.getNonEmptyLines(tempFile)
      
      val expected = Seq("foo", "bar", "baz", "  ", "  blerg  ")
      
      assert(nonEmptyLines === expected)
    }
  }
  
  test("readFrom() and readFromAsUtf8() work the same") {
    val problematic = {
      """|
         |foo
         |bar
         |
         |
         |baz
         |  
         |  blerg  
         |
         |""".stripMargin
    }
    
    withTempFile { tempFile =>                                            
      Files.writeTo(tempFile)(problematic)
      
      assert(Files.readFromAsUtf8(tempFile) === problematic)
      assert(Files.readFrom(tempFile) === problematic)
    }
  }
}

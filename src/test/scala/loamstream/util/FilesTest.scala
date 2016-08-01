package loamstream.util

import org.scalatest.FunSuite
import scala.util.Success
import java.nio.file.Paths
import java.util.stream.Collectors
import java.nio.file.Path

/**
 * @author clint
 * date: Jun 15, 2016
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
    
    val contents = """hello
      world
        blah
        yo  """
    
    assert(Files.readFromAsUtf8(tempFile) == "")
    
    Files.writeTo(tempFile)(contents)
    
    assert(read(tempFile) == contents)
  }
}
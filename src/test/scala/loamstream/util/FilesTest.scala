package loamstream.util

import org.scalatest.FunSuite
import scala.util.Success
import java.nio.file.Paths
import java.util.stream.Collectors

/**
 * @author clint
 * date: Jun 15, 2016
 */
final class FilesTest extends FunSuite {
  test("tempFile()") {
    val path = Files.tempFile("foo")
    
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
  
  test("writeTo()()") {
    val tempFile = Files.tempFile("foo")
    
    def readContents(): String = {
      import java.io._
      
      LoamFileUtils.enclosed(new BufferedReader(new FileReader(tempFile.toFile))) { reader =>
        import scala.collection.JavaConverters._
        
        reader.lines.collect(Collectors.toList()).asScala.mkString("\n")
      }
    }
    
    val contents = """hello
      world
        blah
        yo  """
    
    assert(readContents() == "")
    
    Files.writeTo(tempFile)(contents)
    
    assert(readContents() == contents)
  }
}
package loamstream.util

import org.scalatest.FunSuite
import java.nio.file.Paths
import java.nio.file.FileSystems
import java.io.File

/**
 * @author clint
 * date: Jul 20, 2016
 */
final class PathUtilTest extends FunSuite {

  import PathUtils._
  
  private val add2: String => String = (_: String) + "2"
  
  private val root = Paths.get("/")
  
  private val foo = Paths.get("foo")
  
  private val fooBar = Paths.get("foo/bar")

  // scalastyle:off null
  test("transformFileName") {

    assert(root.getParent == null)
    assert(root.getFileName == null)
    
    assert(transformFileName(root, add2) == null)
    
    assert(foo.getParent == null)
    assert(foo.getFileName != null)
    
    assert(transformFileName(foo, add2) == Paths.get("foo2"))
    
    assert(fooBar.getParent != null)
    assert(fooBar.getFileName != null)
    
    assert(transformFileName(fooBar, add2) == Paths.get("foo/bar2"))
  }
  
  test("getFileNameTransformation") {

    assert(root.getParent == null)
    assert(root.getFileName == null)
    
    assert(getFileNameTransformation(add2)(root) == null)
    
    assert(foo.getParent == null)
    assert(foo.getFileName != null)
    
    assert(getFileNameTransformation(add2)(foo) == Paths.get("foo2"))
    
    assert(fooBar.getParent != null)
    assert(fooBar.getFileName != null)
    
    assert(getFileNameTransformation(add2)(fooBar) == Paths.get("foo/bar2"))
  }
  // scalastyle:on null
  
  test("lastModifiedTime") {
    val doesntExist = Paths.get("/aslkdjklas/lakjslks/askldjlaksd/asklfj")
    
    assert(lastModifiedTime(doesntExist).toEpochMilli == 0L)
    
    val exists = Paths.get("src/test/resources/for-hashing/foo.txt")
    
    assert(lastModifiedTime(exists).toEpochMilli == exists.toFile.lastModified)
  }
  
  test("normalize") {
    val absolute = Paths.get("/x/y/z")

    val rootPath = FileSystems.getDefault.getRootDirectories.iterator.next

    val absoluteExpected = s"${rootPath}x${File.separator}y${File.separator}z"

    import PathUtils.normalize
    
    assert(normalize(absolute) == absoluteExpected)

    val relative = Paths.get("x/y/z")

    //NB: Hopefully this is cross-platform
    val cwd = Paths.get(".").toAbsolutePath.normalize

    val expected = cwd.resolve(relative).toString

    assert(normalize(relative) == expected)
  }
}
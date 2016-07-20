package loamstream.util

import org.scalatest.FunSuite
import java.nio.file.Paths

/**
 * @author clint
 * date: Jul 20, 2016
 */
final class PathUtilTest extends FunSuite {

  import PathUtil._
  
  private val add2: String => String = (_: String) + "2"
  
  private val root = Paths.get("/")
  
  private val foo = Paths.get("foo")
  
  private val fooBar = Paths.get("foo/bar")
  
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
}
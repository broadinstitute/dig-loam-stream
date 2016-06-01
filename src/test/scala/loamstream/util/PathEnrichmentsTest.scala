package loamstream.util

import java.nio.file.Paths
import org.scalatest.FunSuite

/**
 * @author clint
 * date: Jun 1, 2016
 */
final class PathEnrichmentsTest extends FunSuite {
  test("building paths with /") {
    import PathEnrichments._
    
    val root = Paths.get("/")
    
    val opt = root / "opt"
    
    assert(opt == Paths.get("/opt"))
    
    val foo = Paths.get("foo")
    
    val fooBarBazTxt = foo / "bar" / "baz.txt"
    
    assert(fooBarBazTxt == Paths.get("foo/bar/baz.txt"))
  }
}
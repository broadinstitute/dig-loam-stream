package loamstream.util

import java.nio.file.Paths
import org.scalatest.FunSuite
import scala.util.Success

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
  
  test("building paths with / and Tries") {
    import PathEnrichments._
    import Tries._
    
    val root = Paths.get("/")
    
    val opt = root / Success("opt")
    
    assert(opt.get == Paths.get("/opt"))
    
    val foo = Paths.get("foo")
    
    val fooBarBazTxt = foo / Success("bar") / Success("baz.txt")
    
    assert(fooBarBazTxt.get == Paths.get("foo/bar/baz.txt"))
    
    assert((foo / Success("bar") / failure("baz.txt")).isFailure)
    assert((foo / failure("bar") / Success("baz.txt")).isFailure)
    assert((foo / failure("bar") / failure("baz.txt")).isFailure)
  }

  test("appending to paths with + and Tries") {
    import PathEnrichments._
    import Tries._

    val locator = "someDir/someFile"
    val root = Paths.get(locator)
    val ext = ".someExt"

    assert(root + ext === Paths.get(s"$locator.someExt"))

    val foo = Paths.get("foo")
    val fooWithGoodExt = foo + Success(".txt")
    val fooWithBadExt = foo + failure(".txt")

    assert(fooWithGoodExt.get === Paths.get("foo.txt"))
    assert(fooWithBadExt.isFailure)
  }
}

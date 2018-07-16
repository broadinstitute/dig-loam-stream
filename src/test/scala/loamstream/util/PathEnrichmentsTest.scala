package loamstream.util

import org.scalatest.FunSuite
import scala.util.Success
import loamstream.TestHelpers

/**
 * @author clint
 * date: Jun 1, 2016
 */
final class PathEnrichmentsTest extends FunSuite {
  import Paths.Implicits._
  import TestHelpers.path
  
  test("building paths with /") {
    val root = path("/")
    
    val opt = root / "opt"
    
    assert(opt == path("/opt"))
    
    val foo = path("foo")
    
    val fooBarBazTxt = foo / "bar" / "baz.txt"
    
    assert(fooBarBazTxt == path("foo/bar/baz.txt"))
  }
  
  test("building paths with / and Tries") {
    import Tries._
    
    val root = path("/")
    
    val opt = root / Success("opt")
    
    assert(opt.get == path("/opt"))
    
    val foo = path("foo")
    
    val fooBarBazTxt = foo / Success("bar") / Success("baz.txt")
    
    assert(fooBarBazTxt.get == path("foo/bar/baz.txt"))
    
    assert((foo / Success("bar") / failure("baz.txt")).isFailure)
    assert((foo / failure("bar") / Success("baz.txt")).isFailure)
    assert((foo / failure("bar") / failure("baz.txt")).isFailure)
  }

  test("appending to paths with + and Tries") {
    import Tries._

    val locator = "someDir/someFile"
    val root = path(locator)
    val ext = ".someExt"

    assert(root + ext === path(s"$locator.someExt"))

    val foo = path("foo")
    val fooWithGoodExt = foo + Success(".txt")
    val fooWithBadExt = foo + failure(".txt")

    assert(fooWithGoodExt.get === path("foo.txt"))
    assert(fooWithBadExt.isFailure)
  }
}

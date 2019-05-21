package loamstream.util

import java.io.File
import java.nio.file.Path
import java.util.regex.Matcher

import scala.util.Success

import org.scalatest.FunSuite

/**
 * @author clint
 * date: Jun 1, 2016
 */
final class PathsTest extends FunSuite {
  import Paths.Implicits._
  import loamstream.TestHelpers.path
  
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
  
  test("lastModifiedTime") {
    val doesntExist = path("/aslkdjklas/lakjslks/askldjlaksd/asklfj")

    import Paths.lastModifiedTime
    
    assert(lastModifiedTime(doesntExist).toEpochMilli == 0L)

    val exists = path("src/test/resources/for-hashing/foo.txt")

    assert(lastModifiedTime(exists).toEpochMilli == exists.toFile.lastModified)
  }

  test("Root path, new relative path, new absolute path") {
    
    import Paths.getRoot
    import Paths.newRelative
    import Paths.newAbsolute
    
    assert(getRoot.getNameCount === 0)
    if (PlatformUtil.isWindows) {
      assert(getRoot.toString.endsWith(":\\"))
    } else {
      assert(getRoot.toString === "/")
    }
    val relative = newRelative("a", "b", "c")
    assert(!relative.isAbsolute)
    assert(relative.toString.split(Matcher.quoteReplacement(File.separator)) === Array("a", "b", "c"))
    val absolute = newAbsolute("a", "b", "c")
    assert(absolute.isAbsolute)
    assert(absolute.toString.split(Matcher.quoteReplacement(File.separator)).drop(1) === Array("a", "b", "c"))
  }

  test("normalize") {
    import Paths.normalize

    val absolute = path("/x/y/z").toAbsolutePath

    // On windows, multiple drives might exist, and the root drive will be
    // whatever the CWD drive happens to be when the test is run.
    val root = path(".").toAbsolutePath

    // On windows, multiple drives might be in use and the "root" drive used for
    // absolute will be whatever the cwd drive happens to be when the test is
    // run. For this reason, if ANY drive absolute path matches, we're good.
    def matchesExpected(path: Path, expected: String): Boolean = {
      val normalizedPath = normalize(path)
      val expectedPath = normalize(root resolve expected)

      normalizedPath === expectedPath
    }

    assert(matchesExpected(absolute, "/x/y/z"))

    val relative = path("x/y/z")

    assert(matchesExpected(relative, "x/y/z"))

    val hasDot = path("/x/y/./z")

    assert(matchesExpected(hasDot, "/x/y/z"))

    val hasTwoDots = path("/x/y/z/foo/..")

    assert(matchesExpected(hasTwoDots, "/x/y/z"))

    val hasBoth = path("/x/./y/./z/foo/..")

    assert(matchesExpected(hasBoth, "/x/y/z"))
  }
  
  test("mungePathRelatedChars") {
    import Paths.mungePathRelatedChars
    
    assert(mungePathRelatedChars("foo") === "foo")
    assert(mungePathRelatedChars("BAR") === "BAR")
    
    assert(mungePathRelatedChars("x/y/z") === "x_y_z")
    
    assert(mungePathRelatedChars("foo   blah/blah:b$ar\\baz$$") === "foo___blah_blah_b_ar_baz__")
  }
}

package loamstream.util

import java.io.File
import java.nio.file.{FileSystems, Path, Paths}
import java.util.regex.Matcher

import org.scalatest.FunSuite

/**
  * @author clint
  *         date: Jul 20, 2016
  */
final class PathUtilsTest extends FunSuite {

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

  test("Root path, new relative path, new absolute path") {
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
    import PathUtils.normalize

    val absolute = Paths.get("/x/y/z").toAbsolutePath

    // On windows, multiple drives might exist, and the root drive will be
    // whatever the CWD drive happens to be when the test is run.
    val root = Paths.get(".").toAbsolutePath

    // On windows, multiple drives might be in use and the "root" drive used for
    // absolute will be whatever the cwd drive happens to be when the test is
    // run. For this reason, if ANY drive absolute path matches, we're good.
    def matchesExpected(path: Path, expected: String): Boolean = {
      val normalizedPath = normalize(path)
      val expectedPath = normalize(root resolve expected)

      normalizedPath === expectedPath
    }

    assert(matchesExpected(absolute, "/x/y/z"))

    val relative = Paths.get("x/y/z")

    assert(matchesExpected(relative, "x/y/z"))

    val hasDot = Paths.get("/x/y/./z")

    assert(matchesExpected(hasDot, "/x/y/z"))

    val hasTwoDots = Paths.get("/x/y/z/foo/..")

    assert(matchesExpected(hasTwoDots, "/x/y/z"))

    val hasBoth = Paths.get("/x/./y/./z/foo/..")

    assert(matchesExpected(hasBoth, "/x/y/z"))
  }
}
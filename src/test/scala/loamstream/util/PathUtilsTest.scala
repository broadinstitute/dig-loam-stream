package loamstream.util

import java.io.File
import java.nio.file.{FileSystems, Paths}
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
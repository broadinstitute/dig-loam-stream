package loamstream.util

import org.scalatest.FunSuite
import HasPath._

/**
 * @author kyuksel
 *         date: 8/30/17
 */
final class HasPathTest extends FunSuite {
  test("basename") {
    import PathsArePaths._
    import URIsHavePaths._

    assert(basename("/my/folder/myFile.txt") === "myFile.txt")
    assert(basename("gs://my/bucket/myFile.txt") === "myFile.txt")
    assert(basename("/foo/bar/baz/") === "baz")
    assert(basename("/foo/bar/baz//") === "baz")
    assert(basename("/foo/bar/baz//fuz") === "fuz")
    assert(basename("/foo/bar/baz///") === "baz")

    assert(basename("") === "")
    assert(basename("myFile.txt") === "myFile.txt")

    assert(basename("my-folder-myFile.txt", '-') === "myFile.txt")
  }
}

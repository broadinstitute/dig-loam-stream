package loamstream.util

import java.net.URI
import java.nio.file.Paths

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

    assert(base(Paths.get("/my/folder/myFile.txt")) === "myFile.txt")
    assert(base(URI.create("gs://my/bucket/myFile.txt")) === "myFile.txt")
    assert(base(Paths.get("/foo/bar/baz/")) === "baz")
    assert(base(Paths.get("/foo/bar/baz//")) === "baz")
    assert(base(Paths.get("/foo/bar/baz//fuz")) === "fuz")
    assert(base(Paths.get("/foo/bar/baz///")) === "baz")

    assert(base(Paths.get("")) === "")
    assert(base(Paths.get("myFile.txt")) === "myFile.txt")
  }
}

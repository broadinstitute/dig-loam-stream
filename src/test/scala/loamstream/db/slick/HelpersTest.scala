package loamstream.db.slick

import java.nio.file.Paths

import loamstream.util.PathUtils
import org.scalatest.FunSuite

/**
  * @author clint
  *         date: Aug 10, 2016
  */
final class HelpersTest extends FunSuite {
  test("normalize") {
    val absolute = Paths.get("/x/y/z")

    val absoluteExpected = PathUtils.newAbsolute("x", "y", "z").toString

    assert(Helpers.normalize(absolute) == absoluteExpected)

    val relative = Paths.get("x/y/z")

    //NB: Hopefully this is cross-platform
    val cwd = Paths.get(".").toAbsolutePath.normalize

    val expected = cwd.resolve(relative).toString

    assert(Helpers.normalize(relative) == expected)
  }
}
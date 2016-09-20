package loamstream.db.slick

import java.nio.file.Paths

import loamstream.util.OsInfo
import org.scalatest.FunSuite

/**
  * @author clint
  *         date: Aug 10, 2016
  */
final class HelpersTest extends FunSuite {
  test("normalize") {
    val absolute = Paths.get("/x/y/z")

    val expectedAbsolute = if (OsInfo.current.family == OsInfo.Family.Windows) {
      "C:\\x\\y\\z"
    } else {
      "x/y/z"
    }

    assert(Helpers.normalize(absolute) == expectedAbsolute)

    val relative = Paths.get("x/y/z")

    //NB: Hopefully this is cross-platform
    val cwd = Paths.get(".").toAbsolutePath.normalize

    val expected = cwd.resolve(relative).toString

    assert(Helpers.normalize(relative) == expected)
  }
}
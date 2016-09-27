package loamstream.db.slick

import java.io.File
import java.nio.file.{FileSystems, Paths}

import org.scalatest.FunSuite

/**
  * @author clint
  *         date: Aug 10, 2016
  */
final class HelpersTest extends FunSuite {
  test("normalize") {
    val absolute = Paths.get("/x/y/z")

    val rootPath = FileSystems.getDefault.getRootDirectories.iterator.next

    val absoluteExpected = s"${rootPath}x${File.separator}y${File.separator}z"

    assert(Helpers.normalize(absolute) == absoluteExpected)

    val relative = Paths.get("x/y/z")

    //NB: Hopefully this is cross-platform
    val cwd = Paths.get(".").toAbsolutePath.normalize

    val expected = cwd.resolve(relative).toString

    assert(Helpers.normalize(relative) == expected)
  }
}
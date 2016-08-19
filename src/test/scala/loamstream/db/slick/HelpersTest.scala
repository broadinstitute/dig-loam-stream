package loamstream.db.slick

import org.scalatest.FunSuite
import java.nio.file.Paths
import java.io.File
import loamstream.util.PathEnrichments

/**
 * @author clint
 * date: Aug 10, 2016
 */
final class HelpersTest extends FunSuite {
  test("normalize") {
    val absolute = Paths.get("/x/y/z")
    
    assert(Helpers.normalize(absolute) == "/x/y/z")
    
    val relative = Paths.get("x/y/z")
    
    //NB: Hopefully this is cross-platform
    val cwd = Paths.get(".").toAbsolutePath.normalize
    
    val expected = cwd.resolve(relative).toString
    
    assert(Helpers.normalize(relative) == expected)
  }
}
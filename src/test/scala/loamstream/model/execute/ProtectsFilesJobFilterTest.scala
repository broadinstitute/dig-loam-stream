package loamstream.model.execute

import org.scalatest.FunSuite
import java.net.URI
import loamstream.TestHelpers
import java.nio.file.Path

/**
 * @author clint
 * Dec 15, 2020
 */
final class ProtectsFilesJobFilterTest extends FunSuite {
  test("apply - varags") {
    val filter = ProtectsFilesJobFilter("x", "y", "a", "x")
    
    assert(filter.locationsToProtect === Set("x", "y", "a"))
  }
  
  test("apply - Iterable") {
    val filter = ProtectsFilesJobFilter(Iterable("x", "y", "a", "x"))
    
    assert(filter.locationsToProtect === Set("x", "y", "a"))
  }
  
  test("apply - Iterable of Eithers") {
    import URI.{create => uri}
    import TestHelpers.path
    
    val es: Iterable[Either[Path, URI]] = {
      Iterable(Left(path("x")), Right(uri("gs://y")), Left(path("a")), Left(path("x")))
    }
    
    val filter = ProtectsFilesJobFilter(es)
    
    val expected = Set(path("x").toAbsolutePath.toString, "gs://y", path("a").toAbsolutePath.toString)
    
    assert(filter.locationsToProtect === expected)
  }
}

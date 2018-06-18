package loamstream.model.execute

import org.scalatest.FunSuite
import java.nio.file.Path
import loamstream.TestHelpers

/**
 * @author clint
 * Jun 18, 2018
 */
final class LocationsTest extends FunSuite {
  import TestHelpers.path
  
  test("identity - paths") {
    val idPath = Locations.identity[Path]
    
    val p0 = path("foo/bar/baz")
    val p1 = path("/foo/bar/baz")
    val p2 = path("./foo/bar/baz")
    
    assert(idPath.inContainer(p0) === p0)
    assert(idPath.inContainer(p1) === p1)
    assert(idPath.inContainer(p2) === p2)
    
    assert(idPath.inHost(p0) === p0)
    assert(idPath.inHost(p1) === p1)
    assert(idPath.inHost(p2) === p2)
  }
}

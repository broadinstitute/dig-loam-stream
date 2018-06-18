package loamstream.drm.lsf

import org.scalatest.FunSuite
import loamstream.TestHelpers

/**
 * @author clint
 * Jun 18, 2018
 */
final class LsfDockerParamsTest extends FunSuite {
  import TestHelpers.path
  
  private val relativeBase = path("output/dir") 
  private val absoluteBase = path("/output/dir")
  
  private val rel = path("foo/bar/baz")
  private val abs = path("/blerg/zerg")
  
  private val username = System.getProperty("user.name")
  
  test("append") {
    import LsfDockerParams.append
    
    assert(append(relativeBase, rel) === path("output/dir/foo/bar/baz"))
    assert(append(relativeBase, abs) === path("output/dir/blerg/zerg"))
    
    assert(append(absoluteBase, rel) === path("/output/dir/foo/bar/baz"))
    assert(append(absoluteBase, abs) === path("/output/dir/blerg/zerg"))
  }
  
  test("OutputBackend.default") {
    assert(LsfDockerParams.OutputBackend.default === LsfDockerParams.OutputBackend.OutputHpsNobackup)
  }
  
  test("OutputBackend.OutputHpsNobackup.basePath") {
    val expected = path(s"/hps/nobackup/docker/${username}/output")
    
    assert(LsfDockerParams.OutputBackend.OutputHpsNobackup.basePath === expected)
  }
  
  test("OutputBackend.OutputHpsNobackup.name") {
    assert(LsfDockerParams.OutputBackend.OutputHpsNobackup.name === "output_hps_nobackup")
  }
  
  test("guards") {
    LsfDockerParams("library/foo:123", Nil, outputDir = absoluteBase)
    
    intercept[Exception] {
      LsfDockerParams("library/foo:123", Nil, outputDir = relativeBase)
    }
  }
  
  test("inContainer") {
    val params = LsfDockerParams("library/foo:123", Nil, outputDir = absoluteBase)
    
    assert(params.inContainer(rel) === path("/output/dir/foo/bar/baz"))
    assert(params.inContainer(abs) === path("/output/dir/blerg/zerg"))
  }
  
  test("inHost") {
    val params = LsfDockerParams("library/foo:123", Nil, outputDir = absoluteBase)
    
    assert(params.inHost(rel) === path(s"/hps/nobackup/docker/${username}/output/foo/bar/baz"))
    assert(params.inHost(abs) === path(s"/hps/nobackup/docker/${username}/output/blerg/zerg"))
  }
}

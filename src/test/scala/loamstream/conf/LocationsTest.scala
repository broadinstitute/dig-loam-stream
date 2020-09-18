package loamstream.conf

import org.scalatest.FunSuite
import loamstream.TestHelpers

/**
 * @author clint
 * Sep 18, 2020
 */
final class LocationsTest extends FunSuite {
  test("DefaultsIn - default .loamstream dir") {
    import TestHelpers.path
    
    val locs = Locations.DefaultsIn(Locations.Default.loamstreamDir)
    
    assert(locs.loamstreamDir === path(".loamstream"))
    assert(locs.dbDir === path(".loamstream/db"))
    assert(locs.dryRunOutputFile === path(".loamstream/logs/joblist"))
    assert(locs.jobDataDir === path(".loamstream/jobs/data"))
    assert(locs.logDir === path(".loamstream/logs"))
    assert(locs.workerDir === path(".loamstream/workers"))
    assert(locs.lsfDir === path(".loamstream/lsf"))
    assert(locs.ugerDir === path(".loamstream/uger"))
  }
  
  test("DefaultsIn - arbitrary .loamstream dir") {
    import TestHelpers.path
    
    val workDir = path("foo/bar/baz")
    
    val locs = Locations.DefaultsIn(workDir)
    
    assert(locs.loamstreamDir === workDir)
    assert(locs.dbDir === workDir.resolve("db"))
    assert(locs.dryRunOutputFile === workDir.resolve("logs").resolve("joblist"))
    assert(locs.jobDataDir === workDir.resolve("jobs").resolve("data"))
    assert(locs.logDir === workDir.resolve("logs"))
    assert(locs.workerDir === workDir.resolve("workers"))
    assert(locs.lsfDir === workDir.resolve("lsf"))
    assert(locs.ugerDir === workDir.resolve("uger"))
  }
}

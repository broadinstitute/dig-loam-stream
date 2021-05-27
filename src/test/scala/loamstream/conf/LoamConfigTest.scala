package loamstream.conf

import org.scalatest.FunSuite
import loamstream.googlecloud.GoogleCloudConfig
import loamstream.googlecloud.HailConfig

/**
 * @author clint
 * Oct 7, 2020
 */
final class LoamConfigTest extends FunSuite {
  test("defaults") {
    val expected = LoamConfig(
      ugerConfig = Some(UgerConfig()),
      lsfConfig = Some(LsfConfig()),
      slurmConfig = Some(SlurmConfig()),
      googleConfig = None,
      hailConfig = None,
      pythonConfig = None,
      rConfig = None,
      executionConfig = ExecutionConfig.default,
      compilationConfig = CompilationConfig.default,
      drmSystem = None,
      cliConfig = None)
    
    assert(LoamConfig.defaults === expected)
  }
}

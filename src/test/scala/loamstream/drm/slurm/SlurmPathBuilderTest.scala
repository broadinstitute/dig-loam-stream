package loamstream.drm.slurm

import org.scalatest.FunSuite
import loamstream.conf.SlurmConfig
import loamstream.conf.DrmConfig

/**
 * @author clint
 * May 27, 2021
 */
final class SlurmPathBuilderTest extends FunSuite {
  import loamstream.util.BashScript.Implicits._
  import loamstream.TestHelpers.path
  
  private val slurmConfig = SlurmConfig(maxNumJobsPerTaskArray = 42)
  
  private val workDir = slurmConfig.workDir
  
  test("reifyPathTemplate") {
    import SlurmScriptBuilderParams.drmIndexVarExpr

    val template = s"/foo/bar/${drmIndexVarExpr}/baz.${drmIndexVarExpr}"
    
    assert(SlurmPathBuilder.reifyPathTemplate(template, 42) === path("/foo/bar/42/baz.42").toAbsolutePath)
  }
  
  test("pathTemplatePrefix") {
    assert(SlurmPathBuilder.pathTemplatePrefix === "")
  }
  
  test("stdOutPathTemplate") {
    val expected = workDir.resolve("blarg-blahblah/%a.stdout")
    doPathTemplateTest(SlurmPathBuilder.stdOutPathTemplate, expected.render)
  }

  test("stdErrPathTemplate") {
    val expected = workDir.resolve("blarg-blahblah/%a.stderr")
    doPathTemplateTest(SlurmPathBuilder.stdErrPathTemplate, expected.render)
  }

  private def doPathTemplateTest(makeTemplate: (DrmConfig, String) => String, expected: String): Unit = {
    val jobName = "blarg-blahblah"

    val template = makeTemplate(slurmConfig, jobName)

    assert(template === expected)
  }
}
package loamstream.drm.uger

import org.ggf.drmaa.JobTemplate
import org.scalatest.FunSuite

import loamstream.TestHelpers
import loamstream.conf.DrmConfig
import loamstream.conf.UgerConfig


/**
 * @author clint
 * May 11, 2018
 */
final class UgerPathBuilderTest extends FunSuite {
  
  import loamstream.TestHelpers.path
  import loamstream.util.BashScript.Implicits._
  
  private val workDir = TestHelpers.path("/foo/bar/baz").toAbsolutePath

  private val ugerConfig = UgerConfig(workDir = workDir, maxNumJobs = 42)
  
  test("reifyPathTemplate") {
    val template = {
      s":::/foo/bar/${UgerScriptBuilderParams.drmIndexVarExpr}/baz.${UgerScriptBuilderParams.drmIndexVarExpr}"
    }
    
    assert(UgerPathBuilder.reifyPathTemplate(template, 42) === path("/foo/bar/42/baz.42"))
  }
  
  test("pathTemplatePrefix") {
    assert(UgerPathBuilder.pathTemplatePrefix === ":")
  }
  
  test("ugerStdOutPathTemplate") {
    doPathTemplateTest(
      UgerPathBuilder.stdOutPathTemplate,
      s":${workDir.render}/blarg-blahblah.${JobTemplate.PARAMETRIC_INDEX}.stdout")
  }

  test("ugerStdErrPathTemplate") {
    doPathTemplateTest(
      UgerPathBuilder.stdErrPathTemplate,
      s":${workDir.render}/blarg-blahblah.${JobTemplate.PARAMETRIC_INDEX}.stderr")
  }

  private def doPathTemplateTest(makeTemplate: (DrmConfig, String) => String, expected: String): Unit = {
    val jobName = "blarg-blahblah"

    val template = makeTemplate(ugerConfig, jobName)

    assert(template === expected)
  }
}

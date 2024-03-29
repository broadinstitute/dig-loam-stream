package loamstream.drm.lsf

import org.scalatest.FunSuite
import loamstream.TestHelpers
import loamstream.conf.LsfConfig
import loamstream.conf.DrmConfig

/**
 * @author clint
 * May 11, 2018
 */
final class LsfPathBuilderTest extends FunSuite {
  
  import loamstream.util.BashScript.Implicits._
  import TestHelpers.path
  
  private val lsfConfig = LsfConfig(maxNumJobsPerTaskArray = 42)
  
  private val workDir = lsfConfig.workDir
  
  test("reifyPathTemplate") {
    val template = s"/foo/bar/${LsfScriptBuilderParams.drmIndexVarExpr}/baz.${LsfScriptBuilderParams.drmIndexVarExpr}"
    
    assert(LsfPathBuilder.reifyPathTemplate(template, 42) === path("/foo/bar/42/baz.42").toAbsolutePath)
  }
  
  test("pathTemplatePrefix") {
    assert(LsfPathBuilder.pathTemplatePrefix === "")
  }
  
  test("stdOutPathTemplate") {
    val expected = workDir.resolve("blarg-blahblah/%I.stdout")
    doPathTemplateTest(LsfPathBuilder.stdOutPathTemplate, expected.render)
  }

  test("stdErrPathTemplate") {
    val expected = workDir.resolve("blarg-blahblah/%I.stderr")
    doPathTemplateTest(LsfPathBuilder.stdErrPathTemplate, expected.render)
  }

  private def doPathTemplateTest(makeTemplate: (DrmConfig, String) => String, expected: String): Unit = {
    val jobName = "blarg-blahblah"

    val template = makeTemplate(lsfConfig, jobName)

    assert(template === expected)
  }
}

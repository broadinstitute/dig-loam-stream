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
  
  private val workDir = TestHelpers.path("/foo/bar/baz").toAbsolutePath

  private val lsfConfig = LsfConfig(workDir = workDir, maxNumJobs = 42)
  
  test("reifyPathTemplate") {
    val template = s"/foo/bar/${LsfScriptBuilderParams.drmIndexVarExpr}/baz.${LsfScriptBuilderParams.drmIndexVarExpr}"
    
    assert(LsfPathBuilder.reifyPathTemplate(template, 42) === path("/foo/bar/42/baz.42"))
  }
  
  test("pathTemplatePrefix") {
    assert(LsfPathBuilder.pathTemplatePrefix === "")
  }
  
  test("ugerStdOutPathTemplate") {
    doPathTemplateTest(
      LsfPathBuilder.stdOutPathTemplate,
      s"${workDir.render}/blarg-blahblah.%I.stdout")
  }

  test("ugerStdErrPathTemplate") {
    doPathTemplateTest(
      LsfPathBuilder.stdErrPathTemplate,
      s"${workDir.render}/blarg-blahblah.%I.stderr")
  }

  private def doPathTemplateTest(makeTemplate: (DrmConfig, String) => String, expected: String): Unit = {
    val jobName = "blarg-blahblah"

    val template = makeTemplate(lsfConfig, jobName)

    assert(template === expected)
  }
}

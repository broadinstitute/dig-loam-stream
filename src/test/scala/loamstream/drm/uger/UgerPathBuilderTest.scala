package loamstream.drm.uger

import org.scalatest.FunSuite

import loamstream.TestHelpers
import loamstream.conf.DrmConfig
import loamstream.conf.UgerConfig
import loamstream.conf.Locations


/**
 * @author clint
 * May 11, 2018
 */
final class UgerPathBuilderTest extends FunSuite {
  
  import loamstream.TestHelpers.path
  import loamstream.util.BashScript.Implicits._
  
  private val workDir = Locations.Default.ugerDir

  private val ugerConfig = UgerConfig(maxNumJobsPerTaskArray = 42)
  
  private val someDir = path("/some/dir")
  
  private val someEnv = "someEnv"
  
  test("reifyPathTemplate") {
    val params = new UgerScriptBuilderParams(someDir, someEnv)
    
    val pathBuilder = new UgerPathBuilder(params)
    
    val template = {
      s":::/foo/bar/${params.drmIndexVarExpr}/baz.${params.drmIndexVarExpr}"
    }
    
    assert(pathBuilder.reifyPathTemplate(template, 42) === path("/foo/bar/42/baz.42").toAbsolutePath)
  }
  
  test("pathTemplatePrefix") {
    val params = new UgerScriptBuilderParams(someDir, someEnv)
    
    val pathBuilder = new UgerPathBuilder(params)
    
    assert(pathBuilder.pathTemplatePrefix === "")
  }
  
  test("ugerStdOutPathTemplate") {
    val params = new UgerScriptBuilderParams(someDir, someEnv)
    
    val pathBuilder = new UgerPathBuilder(params)
    
    val expected = workDir.resolve("blarg-blahblah.$TASK_ID.stdout")
    
    doPathTemplateTest(pathBuilder.stdOutPathTemplate, expected.render)
  }

  test("ugerStdErrPathTemplate") {
    val params = new UgerScriptBuilderParams(someDir, someEnv)
    
    val pathBuilder = new UgerPathBuilder(params)
    
    val expected = workDir.resolve("blarg-blahblah.$TASK_ID.stderr")
    
    doPathTemplateTest(pathBuilder.stdErrPathTemplate, expected.render)
  }

  private def doPathTemplateTest(makeTemplate: (DrmConfig, String) => String, expected: String): Unit = {
    val jobName = "blarg-blahblah"

    val template = makeTemplate(ugerConfig, jobName)

    assert(template === expected)
  }
}

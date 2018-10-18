package loamstream.drm.uger

import org.scalatest.FunSuite
import loamstream.TestHelpers


/**
 * @author clint
 * May 11, 2018
 */
final class UgerScriptBuilderParamsTest extends FunSuite {
  import TestHelpers.path
  
  private val someDir = path("/some/dir")
  
  private val someEnv = "someEnv"
  
  test("drmIndexVarExpr") {
    val params = new UgerScriptBuilderParams(someDir, someEnv)
    
    assert(params.drmIndexVarExpr === "$drmaa_incr_ph$")
  }
  
  test("preamble") {
    val expected = """|#$ -cwd
                      |
                      |source /broad/software/scripts/useuse
                      |reuse -q UGER
                      |reuse -q Java-1.8
                      |
                      |export PATH=/some/dir:$PATH
                      |source activate someEnv
                      |
                      |mkdir -p /broad/hptmp/${USER}
                      |export SINGULARITY_CACHEDIR=/broad/hptmp/${USER}""".stripMargin
    
    val params = new UgerScriptBuilderParams(someDir, someEnv)
                      
    assert(params.preamble === Some(expected))
  }
  
  test("indexEnvVarName") {
    val params = new UgerScriptBuilderParams(someDir, someEnv)
    
    assert(params.indexEnvVarName === "SGE_TASK_ID")
  }
  
  test("jobIdEnvVarName") {
    val params = new UgerScriptBuilderParams(someDir, someEnv)
    
    assert(params.jobIdEnvVarName === "JOB_ID")
  }
}

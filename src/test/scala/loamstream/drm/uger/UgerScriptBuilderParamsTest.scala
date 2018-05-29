package loamstream.drm.uger

import org.scalatest.FunSuite


/**
 * @author clint
 * May 11, 2018
 */
final class UgerScriptBuilderParamsTest extends FunSuite {
  test("drmIndexVarExpr") {
    assert(UgerScriptBuilderParams.drmIndexVarExpr === "$drmaa_incr_ph$")
  }
  
  test("preamble") {
    val expected = """|#$ -cwd
                      |
                      |source /broad/software/scripts/useuse
                      |reuse -q UGER
                      |reuse -q Java-1.8
                      |
                      |export PATH=/humgen/diabetes/users/dig/miniconda2/bin:$PATH
                      |source activate loamstream_v1.0""".stripMargin
    
    assert(UgerScriptBuilderParams.preamble === Some(expected))
  }
  
  test("indexEnvVarName") {
    assert(UgerScriptBuilderParams.indexEnvVarName === "SGE_TASK_ID")
  }
  
  test("jobIdEnvVarName") {
    assert(UgerScriptBuilderParams.jobIdEnvVarName === "JOB_ID")
  }
}

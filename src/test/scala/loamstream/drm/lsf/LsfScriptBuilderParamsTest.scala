package loamstream.drm.lsf

import org.scalatest.FunSuite

/**
 * @author clint
 * May 11, 2018
 */
final class LsfScriptBuilderParamsTest extends FunSuite {
  test("drmIndexVarExpr") {
    assert(LsfScriptBuilderParams.drmIndexVarExpr === "%I")
  }
  
  test("preamble") {
    assert(LsfScriptBuilderParams.preamble === None)
  }
  
  test("indexEnvVarName") {
    assert(LsfScriptBuilderParams.indexEnvVarName === "LSB_JOBINDEX")
  }
  
  test("jobIdEnvVarName") {
    assert(LsfScriptBuilderParams.jobIdEnvVarName === "LSB_JOBID")
  }
}

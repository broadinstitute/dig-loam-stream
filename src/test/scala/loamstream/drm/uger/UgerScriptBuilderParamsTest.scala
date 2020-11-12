package loamstream.drm.uger

import org.scalatest.FunSuite
import loamstream.TestHelpers

/**
 * @author clint
 * May 11, 2018
 */
final class UgerScriptBuilderParamsTest extends FunSuite {
  import TestHelpers.path
  import loamstream.util.BashScript.Implicits._

  private val someDir = path("/some/dir")

  private val someEnv = "someEnv"

  test("drmIndexVarExpr") {
    val params = new UgerScriptBuilderParams(someDir, someEnv)

    assert(params.drmIndexVarExpr === "$TASK_ID")
  }

  test("preamble") {
    val expected = s"""|#$$ -cwd
                       |
                       |source /broad/software/scripts/useuse
                       |reuse -q UGER
                       |reuse -q Java-1.8
                       |
                       |export PATH=${someDir.render}:$$PATH
                       |source activate someEnv
                       |
                       |mkdir -p /broad/hptmp/$${USER}
                       |export SINGULARITY_CACHEDIR=/broad/hptmp/$${USER}""".stripMargin

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

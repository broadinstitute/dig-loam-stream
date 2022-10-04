package loamstream.drm.slurm

import org.scalatest.FunSuite

/**
 * @author clint
 * May 27, 2021
 */
final class SlurmScriptBuilderParamsTest extends FunSuite {
  test("values") {
    import SlurmScriptBuilderParams._

    assert(preamble === Some("module load singularity/3.6.4"))
    assert(indexEnvVarName === "SLURM_ARRAY_TASK_ID")
    assert(jobIdEnvVarName === "SLURM_ARRAY_JOB_ID")
    assert(drmIndexVarExpr === "%a")
  }
}

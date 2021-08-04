package loamstream.drm.slurm

import loamstream.drm.ScriptBuilderParams

/**
 * @author clint
 * May 18, 2021
 */
object SlurmScriptBuilderParams extends ScriptBuilderParams {
  override def preamble: Option[String] = Some("module load singularity/3.2.1")
  override def indexEnvVarName: String = "SLURM_ARRAY_TASK_ID"
  override def jobIdEnvVarName: String = "SLURM_ARRAY_JOB_ID"
  override def drmIndexVarExpr: String = "%a"
}
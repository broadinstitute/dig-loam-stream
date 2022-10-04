package loamstream.drm.slurm

import loamstream.drm.ScriptBuilderParams
import loamstream.drm.DrmSystem

/**
 * @author clint
 * May 18, 2021
 */
object SlurmScriptBuilderParams extends ScriptBuilderParams {
  override def preamble: Option[String] = Some("module load singularity/3.6.4")
  override def indexEnvVarName: String = "SLURM_ARRAY_TASK_ID"
  override def jobIdEnvVarName: String = "SLURM_ARRAY_JOB_ID"
  override def drmIndexVarExpr: String = "%a"

  override def drmSystem: DrmSystem = DrmSystem.Slurm
}

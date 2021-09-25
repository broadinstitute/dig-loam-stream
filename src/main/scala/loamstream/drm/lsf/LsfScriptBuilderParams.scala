package loamstream.drm.lsf

import loamstream.drm.ScriptBuilderParams
import loamstream.drm.DrmSystem

/**
 * @author clint
 * May 11, 2018
 */
object LsfScriptBuilderParams extends ScriptBuilderParams {
  override val preamble: Option[String] = None 
  override val indexEnvVarName: String = "LSB_JOBINDEX" 
  override val jobIdEnvVarName: String = "LSB_JOBID"
  override val drmIndexVarExpr: String = "%I"

  override def drmSystem: DrmSystem = DrmSystem.Lsf
}

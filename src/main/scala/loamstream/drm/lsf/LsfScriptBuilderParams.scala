package loamstream.drm.lsf

import loamstream.drm.ScriptBuilderParams

/**
 * @author clint
 * May 11, 2018
 */
object LsfScriptBuilderParams extends ScriptBuilderParams {
  override val preamble = None 
  override val indexEnvVarName = "LSB_JOBINDEX" 
  override val jobIdEnvVarName = "LSB_JOBID"
  override val drmIndexVarExpr = "%I"
}

package loamstream.drm.slurm

import loamstream.drm.ScriptBuilderParams

/**
 * @author clint
 * May 18, 2021
 */
object SlurmScriptBuilderParams extends ScriptBuilderParams {
  override def preamble: Option[String] = ???
  override def indexEnvVarName: String = ???
  override def jobIdEnvVarName: String = ???
  override def drmIndexVarExpr: String = ???
}
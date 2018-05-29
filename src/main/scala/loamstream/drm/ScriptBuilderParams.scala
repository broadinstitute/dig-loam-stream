package loamstream.drm

import java.nio.file.Path
import loamstream.conf.DrmConfig
import loamstream.util.BashScript

/**
 * @author clint
 * May 11, 2018
 */
trait ScriptBuilderParams {
  def preamble: Option[String] 
  def indexEnvVarName: String 
  def jobIdEnvVarName: String
  def drmIndexVarExpr: String
}

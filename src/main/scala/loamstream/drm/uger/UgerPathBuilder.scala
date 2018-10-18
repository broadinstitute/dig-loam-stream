package loamstream.drm.uger

import java.nio.file.Path
import java.nio.file.Paths

import loamstream.drm.PathBuilder
import loamstream.drm.ScriptBuilderParams

/**
 * @author clint
 * May 11, 2018
 */
final case class UgerPathBuilder(override val scriptBuilderParams: UgerScriptBuilderParams) extends PathBuilder {
  override def reifyPathTemplate(template: String, drmIndex: Int): Path = {
    //NB: Replace task-array-index placeholder, drop initial ':'
    val pathString = template.replace(scriptBuilderParams.drmIndexVarExpr, drmIndex.toString).dropWhile(_ == ':')

    Paths.get(pathString).toAbsolutePath
  }
  
  //NB: Need to build this differently by environment (':' for Uger, '' for LSF)
  override def pathTemplatePrefix: String = ":"
}

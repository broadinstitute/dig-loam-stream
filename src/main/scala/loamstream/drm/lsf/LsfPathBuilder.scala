package loamstream.drm.lsf

import loamstream.drm.PathBuilder
import loamstream.drm.ScriptBuilderParams
import java.nio.file.Paths
import java.nio.file.Path

/**
 * @author clint
 * May 11, 2018
 */
object LsfPathBuilder extends PathBuilder {
  
  override def reifyPathTemplate(template: String, drmIndex: Int): Path = {
    //NB: Replace task-array-index placeholder
    val pathString = template.replace(scriptBuilderParams.drmIndexVarExpr, drmIndex.toString)

    Paths.get(pathString).toAbsolutePath
  }
    
  //NB: Need to build this differently by environment (':' for Uger, '' for LSF)
  override def pathTemplatePrefix: String = ""
  
  override def scriptBuilderParams: ScriptBuilderParams = LsfScriptBuilderParams
}

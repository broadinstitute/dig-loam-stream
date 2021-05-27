package loamstream.drm.slurm

import java.nio.file.Path

import loamstream.drm.PathBuilder
import loamstream.drm.ScriptBuilderParams
import java.nio.file.Paths

/**
 * @author clint
 * May 18, 2021
 */
object SlurmPathBuilder extends PathBuilder {
  override def reifyPathTemplate(template: String, drmIndex: Int): Path = {
    //NB: Replace task-array-index placeholder
    val pathString = template.replace(scriptBuilderParams.drmIndexVarExpr, drmIndex.toString)

    Paths.get(pathString).toAbsolutePath
  }
    
  //NB: Need to build this differently by environment (':' for Uger, '' for LSF and Slurm)
  override def pathTemplatePrefix: String = ""
  
  override def scriptBuilderParams: ScriptBuilderParams = SlurmScriptBuilderParams
}
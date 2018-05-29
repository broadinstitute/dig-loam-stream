package loamstream.drm

import java.nio.file.Path
import loamstream.conf.DrmConfig
import loamstream.util.BashScript

/**
 * @author clint
 * May 11, 2018
 */
trait PathBuilder {
  def reifyPathTemplate(template: String, drmIndex: Int): Path
    
  //NB: Need to build this differently by environment (':' for Uger, '' for LSF)
  def pathTemplatePrefix: String
  
  def scriptBuilderParams: ScriptBuilderParams
  
  final def stdOutPathTemplate(drmConfig: DrmConfig, jobName: String): String = {
    makeErrorOrOutputPath(drmConfig, jobName, "stdout")
  }

  final def stdErrPathTemplate(drmConfig: DrmConfig, jobName: String): String = {
    makeErrorOrOutputPath(drmConfig, jobName, "stderr")
  }

  private def makeErrorOrOutputPath(drmConfig: DrmConfig, jobName: String, suffix: String): String = {
    import BashScript.Implicits._
      
    //scriptBuilderParams.drmIndexVarExpr is a special string that will be substituted with the index of a job
    //in a task/job array at execution time.  
    //LSF uses '%I', Uger uses JobTemplate.PARAMETRIC_INDEX ('$drmaa_incr_ph$')
    BashScript.escapeString(s"${pathTemplatePrefix}${drmConfig.workDir.render}/$jobName.${scriptBuilderParams.drmIndexVarExpr}.$suffix")
  }
}

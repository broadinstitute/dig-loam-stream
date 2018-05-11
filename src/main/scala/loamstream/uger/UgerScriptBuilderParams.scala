package loamstream.uger

import org.ggf.drmaa.JobTemplate

import loamstream.drm.ScriptBuilderParams

/**
 * @author clint
 * May 11, 2018
 */
object UgerScriptBuilderParams extends ScriptBuilderParams {
  
  //NB: We need to 'use' Java-1.8 to make some steps of the QC pipeline work.
  private val ugerPreamble="""|#$ -cwd
                              |
                              |source /broad/software/scripts/useuse
                              |reuse -q UGER
                              |reuse -q Java-1.8
                              |
                              |export PATH=/humgen/diabetes/users/dig/miniconda2/bin:$PATH
                              |source activate loamstream_v1.0""".stripMargin
  
  override val preamble = Option(ugerPreamble) 
  override val indexEnvVarName = "SGE_TASK_ID" 
  override val jobIdEnvVarName = "JOB_ID"
  override val drmIndexVarExpr = JobTemplate.PARAMETRIC_INDEX
}

package loamstream.drm.uger

import org.ggf.drmaa.JobTemplate
import loamstream.drm.ScriptBuilderParams
import java.nio.file.Path
import loamstream.conf.UgerConfig

/**
 * @author clint
 * May 11, 2018
 */
final case class UgerScriptBuilderParams(extraPathDir: Path, condaEnvName: String) extends ScriptBuilderParams {
  import loamstream.util.BashScript.Implicits._

  /*
   * We need to 'use' Java-1.8 to make some steps of the QC pipeline work.
   *
   * Set SINGULARITY_CACHEDIR to something other than the default, under
   * ~/.singularity, which will cause quota problems.  Follow recommendations
   * from BITS at
   * https://broad.service-now.com/nav_to.do?uri=%2Fkb_view.do%3Fsysparm_article%3DKB0010821
   */
  private def ugerPreamble = s"""|#$$ -cwd
                                 |
                                 |source /broad/software/scripts/useuse
                                 |reuse -q UGER
                                 |reuse -q Java-1.8
                                 |
                                 |export PATH=${extraPathDir.render}:$$PATH
                                 |source activate ${condaEnvName}
                                 |
                                 |mkdir -p /broad/hptmp/$${USER}
                                 |export SINGULARITY_CACHEDIR=/broad/hptmp/$${USER}""".stripMargin

  override val preamble: Option[String] = Option(ugerPreamble)
  override val indexEnvVarName: String = "SGE_TASK_ID"
  override val jobIdEnvVarName: String = "JOB_ID"
  override val drmIndexVarExpr: String = JobTemplate.PARAMETRIC_INDEX
}

object UgerScriptBuilderParams {
  def apply(ugerConfig: UgerConfig): UgerScriptBuilderParams = {
    new UgerScriptBuilderParams(ugerConfig.extraPathDir, ugerConfig.condaEnvName)
  }
}

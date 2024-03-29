package loamstream.googlecloud

import java.io.File

import org.scalatest.FunSuite

import loamstream.TestHelpers
import loamstream.conf.LoamConfig
import loamstream.loam.LoamCmdTool
import loamstream.loam.LoamProjectContext
import loamstream.loam.LoamScriptContext
import loamstream.loam.LoamToolBox
import loamstream.model.execute.GoogleSettings
import loamstream.model.jobs.commandline.CommandLineJob
import loamstream.util.BashScript.Implicits.BashPath
import loamstream.util.Files
import loamstream.model.execute.LocalSettings
import loamstream.model.execute.Settings
import loamstream.conf.LsSettings

/**
 * @author clint
 *         Feb 23, 2017
 */
final class HailSupportTest extends FunSuite {

  import loamstream.TestHelpers.path

  //NB: Write pyhail script files to a temp dir
  private val scriptDir = TestHelpers.getWorkDir(getClass.getSimpleName)

  private val clusterId = "cluster-asdfasdf"
  private val projectId = "project-kajsdhka"
  private val region = "region-kasdhasfd"
  
  //NB: Write pyhail script files to a temp dir
  private lazy val config: LoamConfig = {
    val configString = {
      s"""
      loamstream {
        googlecloud {
          gcloudBinary = "/path/to/gcloud"
          gsutilBinary = "/path/to/gsutil"
          projectId = "${projectId}"
          clusterId = "${clusterId}"
          credentialsFile = "/path/to/creds"
          region = "${region}"

          hail {
            condaEnv = "hail-0.2.18"
            scriptDir = "${scriptDir.render}" //don't litter in the current dir
          }
        }
      }"""
    }

    LoamConfig.fromString(configString).get
  }

  private def projectContext: LoamProjectContext = LoamProjectContext.empty(config, LsSettings.noCliConfig)

  private val googleSettings = GoogleSettings(clusterId, clusterConfig = ClusterConfig.default)

  // scalastyle:off line.size.limit
  private val sep = File.separator
  private val gCloudPath = s"${sep}path${sep}to${sep}gcloud"
  private val hailctlPrefix = s"""source ~/.bashrc && conda activate "hail-0.2.18" && export CLOUDSDK_CORE_PROJECT="${projectId}" && export CLOUDSDK_DATAPROC_REGION="${region}" && export PATH="/path/to":$${PATH} && hailctl dataproc submit ${clusterId} """
  // scalastyle:on line.size.limit

  import HailSupport._

  test("Guards: executionEnvironment") {
    withScriptContext(projectContext, initialSettings = googleSettings) { implicit scriptContext =>

      hail""

      pyhail""

      scriptContext.settings = LocalSettings

      intercept[Exception] {
        hail""
      }

      intercept[Exception] {
        pyhail""
      }

      scriptContext.settings = TestHelpers.defaultUgerSettings

      intercept[Exception] {
        hail""
      }

      intercept[Exception] {
        pyhail""
      }

      scriptContext.settings = googleSettings

      hail""

      pyhail""
    }
  }

  test("Guards: config sections") {
    withScriptContext(LoamProjectContext.empty(config, LsSettings.noCliConfig)) { implicit scriptContext =>
      hail""
    }

    withScriptContext(LoamProjectContext.empty(config, LsSettings.noCliConfig)) { implicit scriptContext =>
      pyhail""
    }

    val noGoogleConfig = config.copy(googleConfig = None)

    withScriptContext(LoamProjectContext.empty(noGoogleConfig, LsSettings.noCliConfig)) { implicit scriptContext =>
      intercept[Exception] {
        hail""
      }

      intercept[Exception] {
        pyhail""
      }
    }

    val noHailConfig = config.copy(hailConfig = None)

    withScriptContext(LoamProjectContext.empty(noHailConfig, LsSettings.noCliConfig)) { implicit scriptContext =>
      intercept[Exception] {
        hail""
      }

      intercept[Exception] {
        pyhail""
      }
    }
  }

  test("Hail interpolator works with an empty command") {
    val expectedCommandLine = hailctlPrefix

    withScriptContext { implicit context =>
      doTest(expectedCommandLine)(hail"")
    }
  }

  test("Hail interpolator works with a simple command") {
    val expectedCommandLine = s"${hailctlPrefix}foo.py"

    withScriptContext { implicit context =>
      doTest(expectedCommandLine)(hail"foo.py")
    }
  }

  test("PyHail interpolator works with a simple inline script") {
    withScriptContext { implicit context =>
      val tool = pyhail"""xyz
  asdf
  foo"""

      val commandLine = tool.commandLine

      val scriptFile = commandLine.split("\\s+").last

      val expectedCommandLine = s"${hailctlPrefix}${scriptFile}"

      assert(commandLine === expectedCommandLine)

      val scriptContents = Files.readFrom(scriptFile)

      val expectedScriptContents = """xyz
  asdf
  foo"""

      assert(scriptContents === expectedScriptContents)
    }
  }

  test("Hail interpolator works with a real-world command") {
    val ancestryLog = path("some/log/file")
    val inKgHail = path("in/kg/hail")
    val vds = path("some/vds/dir")
    val inKgV35kAf = path("in/kg/V35kAf")
    val ancestryScoresTsv = path("ancestry/scores.tsv")
    val ancestryPcaLoadingsTsv = path("ancestry/pca/loadings.tsv")

    // scalastyle:off line.size.limit
    val expectedCommandLine = {
      val suffix: String = {
        s"""-l ${ancestryLog}
           | read ${inKgHail}
           | put -n KG
           | read -i ${vds}
           | join --right KG
           | annotatevariants table ${inKgV35kAf}
           | -e Variant
           | -c "va.refPanelAF = table.refPanelAF"
           | --impute
           | pca -k 10
           | --scores sa.pca.scores
           | --eigenvalues global.pca.evals
           | --loadings va.pca.loadings
           | exportsamples -c "IID = sa.pheno.IID, POP = sa.pheno.POP, SUPERPOP = sa.pheno.SUPERPOP, SEX = sa.pheno.SEX, PC1 = sa.pca.scores.PC1, PC2 = sa.pca.scores.PC2, PC3 = sa.pca.scores.PC3, PC4 = sa.pca.scores.PC4, PC5 = sa.pca.scores.PC5, PC6 = sa.pca.scores.PC6, PC7 = sa.pca.scores.PC7, PC8 = sa.pca.scores.PC8, PC9 = sa.pca.scores.PC9, PC10 = sa.pca.scores.PC10"
           | -o ${ancestryScoresTsv}
           | exportvariants -c "ID = v, PC1 = va.pca.loadings.PC1, PC2 = va.pca.loadings.PC2, PC3 = va.pca.loadings.PC3, PC4 = va.pca.loadings.PC4, PC5 = va.pca.loadings.PC5, PC6 = va.pca.loadings.PC6, PC7 = va.pca.loadings.PC7, PC8 = va.pca.loadings.PC8, PC9 = va.pca.loadings.PC9, PC10 = va.pca.loadings.PC10"
           | -o ${ancestryPcaLoadingsTsv}""".stripMargin.trim
      }

      s"${hailctlPrefix}${suffix}"
    }

    withScriptContext { implicit context =>
      doTest(expectedCommandLine) {
        hail"""
        -l ${ancestryLog}
        read ${inKgHail}
        put -n KG
        read -i ${vds}
        join --right KG
        annotatevariants table ${inKgV35kAf}
        -e Variant
        -c "va.refPanelAF = table.refPanelAF"
        --impute
        pca -k 10
        --scores sa.pca.scores
        --eigenvalues global.pca.evals
        --loadings va.pca.loadings
        exportsamples -c "IID = sa.pheno.IID, POP = sa.pheno.POP, SUPERPOP = sa.pheno.SUPERPOP, SEX = sa.pheno.SEX, PC1 = sa.pca.scores.PC1, PC2 = sa.pca.scores.PC2, PC3 = sa.pca.scores.PC3, PC4 = sa.pca.scores.PC4, PC5 = sa.pca.scores.PC5, PC6 = sa.pca.scores.PC6, PC7 = sa.pca.scores.PC7, PC8 = sa.pca.scores.PC8, PC9 = sa.pca.scores.PC9, PC10 = sa.pca.scores.PC10"
        -o ${ancestryScoresTsv}
        exportvariants -c "ID = v, PC1 = va.pca.loadings.PC1, PC2 = va.pca.loadings.PC2, PC3 = va.pca.loadings.PC3, PC4 = va.pca.loadings.PC4, PC5 = va.pca.loadings.PC5, PC6 = va.pca.loadings.PC6, PC7 = va.pca.loadings.PC7, PC8 = va.pca.loadings.PC8, PC9 = va.pca.loadings.PC9, PC10 = va.pca.loadings.PC10"
        -o ${ancestryPcaLoadingsTsv}"""
      }
      // scalastyle:on line.size.limit
    }
  }

  test("PyHail interpolator works with a real-world inline script") {
    withScriptContext { implicit context =>
      // scalastyle:off line.size.limit
      val tool = pyhail"""from hail import *
  hc = HailContext()

  vds = hc.import_vcf('/some/really/long/path/to/a/file/ALL.purcell5k.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf.bgz')
  vds = vds.split_multi()
  vds.write('1kg_purcell.vds',overwrite=True)

  vds = hc.read('1kg_purcell.vds')
  vds.summarize().report()

  vds.export_plink('1kg_purcell',fam_expr='famID=s,id=s')"""

      val commandLine = tool.commandLine

      val scriptFile = commandLine.split("\\s+").last

      val expectedCommandLine = s"${hailctlPrefix}${scriptFile}"

      assert(commandLine === expectedCommandLine)

      val scriptContents = Files.readFrom(scriptFile)

      val expectedScriptContents = """from hail import *
  hc = HailContext()

  vds = hc.import_vcf('/some/really/long/path/to/a/file/ALL.purcell5k.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf.bgz')
  vds = vds.split_multi()
  vds.write('1kg_purcell.vds',overwrite=True)

  vds = hc.read('1kg_purcell.vds')
  vds.summarize().report()

  vds.export_plink('1kg_purcell',fam_expr='famID=s,id=s')"""
      // scalastyle:on line.size.limit

      assert(scriptContents === expectedScriptContents)
    }
  }

  private def doTest(expectedCommandLine: String)(actual: => LoamCmdTool)(implicit sc: LoamScriptContext): Unit = {
    val tool = actual

    val graph = sc.projectContext.graph

    val toolBox = new LoamToolBox()

    val executable = toolBox.createExecutable(graph)
    
    assert(executable.jobNodes.size === 1, s"Expected only one job, but got ${executable.jobNodes}")
    
    val job = toolBox.createExecutable(graph).jobNodes.head.job.asInstanceOf[CommandLineJob]

    def collapseWhitespace(s: String) = s.replaceAll("\\s+", " ")

    assert(collapseWhitespace(job.commandLineString) === collapseWhitespace(expectedCommandLine))
  }

  private def withScriptContext[A](f: LoamScriptContext => A): A = withScriptContext(projectContext, googleSettings)(f)

  private def withScriptContext[A](
    projectContext: LoamProjectContext,
    initialSettings: Settings = googleSettings)(f: LoamScriptContext => A): A = {

    val scriptContext = new LoamScriptContext(projectContext)

    scriptContext.settings = initialSettings

    f(scriptContext)
  }
}

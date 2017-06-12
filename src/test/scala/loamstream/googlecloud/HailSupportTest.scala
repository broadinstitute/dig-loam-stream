package loamstream.googlecloud

import java.io.File

import com.typesafe.config.ConfigFactory
import loamstream.conf.{ExecutionConfig, LoamConfig}
import loamstream.loam.{LoamCmdTool, LoamProjectContext, LoamScriptContext, LoamToolBox}
import loamstream.model.jobs.commandline.CommandLineJob
import org.scalatest.FunSuite
import loamstream.model.execute.ExecutionEnvironment

/**
  * @author clint
  *         Feb 23, 2017
  */
final class HailSupportTest extends FunSuite {

  import loamstream.TestHelpers.path

  private val projectContext = LoamProjectContext.empty(config)

  private[this] implicit val scriptContext = {
    val sc = new LoamScriptContext(projectContext)
    
    sc.executionEnvironment = ExecutionEnvironment.Google
    
    sc
  }

  // scalastyle:off line.size.limit
  private val sep = File.separator
  private val gCloudPath = s"${sep}path${sep}to${sep}gcloud"
  private val googlePrefix = s"""$gCloudPath dataproc jobs submit pyspark --cluster=some-cluster-id --files=gs://some-bucket/hail-all-spark.jar --py-files=gs://some-bucket/hail-all.zip --properties="spark.driver.extraClassPath=./hail-all-spark.jar,spark.executor.extraClassPath=./hail-all-spark.jar" """
  // scalastyle:on line.size.limit

  import HailSupport._

  test("Guards: executionEnvironment") {
    withScriptContext(LoamProjectContext.empty(config)) { implicit scriptContext => 
    
      scriptContext.executionEnvironment = ExecutionEnvironment.Google
      
      hail""
      
      scriptContext.executionEnvironment = ExecutionEnvironment.Local
      
      intercept[Exception] {
        hail""
      }
      
      scriptContext.executionEnvironment = ExecutionEnvironment.Uger
      
      intercept[Exception] {
        hail""
      }
    
      scriptContext.executionEnvironment = ExecutionEnvironment.Google
      
      hail""
    }
  }
  
  test("Guards: config sections") {
    withScriptContext(LoamProjectContext.empty(config)) { implicit scriptContext =>
      hail""
    }
    
    val noGoogleConfig = config.copy(googleConfig = None)
    
    withScriptContext(LoamProjectContext.empty(noGoogleConfig)) { implicit scriptContext =>
      intercept[Exception] {
        hail""
      }
    }
    
    val noHailConfig = config.copy(hailConfig = None)
    
    withScriptContext(LoamProjectContext.empty(noHailConfig)) { implicit scriptContext =>
      intercept[Exception] {
        hail""
      }
    }
  }
  
  test("Hail interpolator works with an empty command") {
    val expectedCommandLine = googlePrefix

    doTest(expectedCommandLine)(hail"")
  }

  test("Hail interpolator works with a simple command") {
    val expectedCommandLine = s"${googlePrefix}foo.py"

    doTest(expectedCommandLine)(hail"foo.py")
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

      s"${googlePrefix}${suffix}"
    }

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

  private def doTest(expectedCommandLine: String)(actual: => LoamCmdTool)(implicit sc: LoamScriptContext): Unit = {
    val toolBox = new LoamToolBox(sc.projectContext)

    val job = toolBox.toolToJobShot(actual).get.asInstanceOf[CommandLineJob]

    def collapseWhitespace(s: String) = s.replaceAll("\\s+", " ")

    assert(collapseWhitespace(job.commandLineString) === collapseWhitespace(expectedCommandLine))
  }
  
  import ExecutionEnvironment.Google
  
  private def withScriptContext[A](
      projectContext: LoamProjectContext,
      initialExecutionEnvironment: ExecutionEnvironment = Google)(f: LoamScriptContext => A): A = {
    
    val scriptContext = new LoamScriptContext(projectContext)
    
    scriptContext.executionEnvironment = initialExecutionEnvironment
    
    f(scriptContext)
  }

  private lazy val config: LoamConfig = {
    val configString =
      """
      loamstream {
        googlecloud {
          gcloudBinary = "/path/to/gcloud"
          projectId = "some-project-id"
          clusterId = "some-cluster-id"
          credentialsFile = "/path/to/creds"

          hail {
            jar = "gs://some-bucket/hail-all-spark.jar"
            zip = "gs://some-bucket/hail-all.zip"
          }
        }
      }"""

    val typesafeConfig = ConfigFactory.parseString(configString)

    val googleConfig = GoogleCloudConfig.fromConfig(typesafeConfig)

    val hailConfig = HailConfig.fromConfig(typesafeConfig)

    LoamConfig(
      ugerConfig = None,
      googleConfig.toOption,
      hailConfig.toOption,
      pythonConfig = None,
      rConfig = None,
      executionConfig = ExecutionConfig.default)
  }
}

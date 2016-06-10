package loamstream

import scala.util.Try

import org.scalatest.FunSuite

import loamstream.model.execute.LeavesFirstExecuter
import loamstream.model.jobs.LJob
import loamstream.model.jobs.LToolBox
import loamstream.pipelines.qc.ancestry.AncestryInferencePipeline
import loamstream.tools.core.LCoreEnv.Keys
import loamstream.tools.klusta.KlustaKwikLineCommand
import loamstream.util.Hit
import loamstream.util.Shot
import loamstream.util.StringUtils
import tools.PcaWeightsReader
import tools.core.{CoreToolBox, LCoreDefaultStoreIds}
import tools.klusta.KlustaKwikKonfig

/**
  * LoamStream
  * Created by oliverr on 4/8/2016.
  */
final class AncestryInferenceEndToEndTest extends FunSuite {
  //TODO: Make this field unnecessary via CI
  @deprecated("", "")
  val canRunKlustaKwik = isKlustaKwikPresent
  
  private val executer = {
    import scala.concurrent.ExecutionContext.Implicits.global
      
    new LeavesFirstExecuter
  }
  
  test("creating jobs from inference tools.") {
    val (toolbox, pipeline) = makeToolBoxAndPipeline()

    val pcaProjectionJobsShot = toolbox.createJobs(pipeline.pcaProjectionTool, pipeline)
    
    //NB: Try a pattern-match to get a better error message on failures
    val Hit(Seq(pcaProjectionJob)) = pcaProjectionJobsShot.map(_.toSeq)
    
    val sampleClusteringJobsShot = toolbox.createJobs(pipeline.sampleClusteringTool, pipeline)

    //NB: Try a pattern-match to get a better error message on failures
    val Hit(Seq(sampleClusteringJob)) = sampleClusteringJobsShot.map(_.toSeq)
  }
  
  test("Running ancestry inference pipeline. (AST)") {
    val (toolbox, pipeline) = makeToolBoxAndPipeline()
    
    val executable = toolbox.createExecutable(pipeline.ast)

    val jobResults = executer.execute(executable)

    checkResults(jobResults)
  }

  private def checkResults(results: Map[LJob, Shot[LJob.Result]]): Unit = {
    // TODO: Should be able to calculate the number of results expected form number of jobs
    val numResultsExpected = if(canRunKlustaKwik) 4 else 3 //scalastyle:ignore

    assert(results.size === numResultsExpected)
      
    //TODO: More-explicit assertion for better failure messages
    assert(allSuccesses(results.values))
  }
  
  private def allSuccesses(shots: Iterable[Shot[LJob.Result]]): Boolean = {
    shots.forall {
      case Hit(r) => r.isSuccess
      case _ => false
    }
  }
  
  private def makeToolBoxAndPipeline(): (LToolBox, AncestryInferencePipeline) = {

    val miniVcfFilePath = TestData.sampleFiles.miniVcfOpt.get
    val vcfFiles = Seq(StringUtils.pathTemplate(miniVcfFilePath.toString, "XXX"))

    val props = TestData.props

    val pcaWeightsFile = PcaWeightsReader.weightsFilePath(props).get
    
    val klustaConfig = KlustaKwikKonfig.withTempWorkDir("data")
    
    val env = LEnv(
      Keys.genotypesId -> LCoreDefaultStoreIds.genotypes,
      Keys.pcaWeightsId -> LCoreDefaultStoreIds.pcaWeights)

    val pipeline = AncestryInferencePipeline(miniVcfFilePath, pcaWeightsFile, klustaConfig)
    val toolbox = CoreToolBox(env)
    
    (toolbox, pipeline)
  }
  
  //Returns true if the command for KlustaKwik, as specified by KlustaKwikLineCommand.name is on the path
  //and runnable; returns false otherwise.
  private def isKlustaKwikPresent: Boolean = {
    import scala.sys.process._
    
    val command = KlustaKwikLineCommand.name
    
    val processLogger = ProcessLogger(_ => (), _ => ())
    
    Try(command.!(processLogger)).isSuccess
  }
}

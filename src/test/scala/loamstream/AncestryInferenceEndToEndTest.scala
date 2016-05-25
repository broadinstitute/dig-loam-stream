package loamstream

import org.scalatest.FunSuite

import loamstream.apps.minimal.MiniExecuter
import loamstream.model.jobs.LJob
import loamstream.model.jobs.LToolBox
import loamstream.pipelines.qc.ancestry.AncestryInferencePipeline
import loamstream.tools.core.LCoreEnv.Keys
import loamstream.util.FileAsker
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
  private val canRunKlustaKwik = true
  
  test("Running ancestry inference pipeline. (LPipeline)") {
    val (toolbox, pipeline) = makeToolBoxAndPipeline()

    val pcaProjectionJobsShot = toolbox.createJobs(pipeline.pcaProjectionTool, pipeline)
    
    //NB: Try a pattern-match to get a better error message on failures
    val Hit(Seq(pcaProjectionJob)) = pcaProjectionJobsShot.map(_.toSeq)
    
    val sampleClusteringJobsShot = toolbox.createJobs(pipeline.sampleClusteringTool, pipeline)

    //NB: Try a pattern-match to get a better error message on failures
    val Hit(Seq(sampleClusteringJob)) = sampleClusteringJobsShot.map(_.toSeq)
    
    val executable = toolbox.createExecutable(pipeline)
    
    if (canRunKlustaKwik) {
      val jobResults = MiniExecuter.execute(executable)
      
      checkResults(jobResults)
    }
  }
  
  test("Running ancestry inference pipeline. (AST)") {
    val (toolbox, pipeline) = makeToolBoxAndPipeline()

    val executable = toolbox.createExecutable(pipeline.ast)
    
    println(s"Made ${executable.jobs.size} jobs:")
    
    executable.jobs.foreach(_.print())
    
    if (canRunKlustaKwik) {
      val jobResults = MiniExecuter.execute(executable)
      
      checkResults(jobResults)
    }
  }

  private def checkResults(results: Map[LJob, Shot[LJob.Result]]): Unit = {
    println(results)
    
    assert(results.size === 4)
      
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
}

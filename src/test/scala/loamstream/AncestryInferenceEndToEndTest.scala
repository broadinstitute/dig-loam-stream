package loamstream

import org.scalatest.FunSuite

import loamstream.apps.minimal.MiniExecuter

import loamstream.pipelines.qc.ancestry.AncestryInferencePipeline
import loamstream.tools.PcaWeightsReader
import loamstream.tools.core.{CoreToolBox, LCoreDefaultStoreIds}
import loamstream.tools.core.LCoreEnv.Keys
import loamstream.tools.klusta.KlustaKwikKonfig
import loamstream.util.{FileAsker, Hit, StringUtils}

/**
  * LoamStream
  * Created by oliverr on 4/8/2016.
  */
final class AncestryInferenceEndToEndTest extends FunSuite {
  test("Going through ancestry inference pipeline without crashing.") {
    import TestData.sampleFiles

    val miniVcfFilePath = sampleFiles.miniVcfOpt.get
    val vcfFiles = Seq(StringUtils.pathTemplate(miniVcfFilePath.toString, "XXX"))

    val props = TestData.props

    val pcaWeightsFile = PcaWeightsReader.weightsFilePath(props).get
    
    val klustaConfig = KlustaKwikKonfig.withTempWorkDir("data")
    
    val env = LEnv(
      Keys.genotypesId -> LCoreDefaultStoreIds.genotypes,
      Keys.pcaWeightsId -> LCoreDefaultStoreIds.pcaWeights)

    val pipeline = AncestryInferencePipeline(miniVcfFilePath, pcaWeightsFile, klustaConfig)
    val toolbox = CoreToolBox(env)

    val pcaProjectionJobsShot = toolbox.createJobs(pipeline.pcaProjectionTool, pipeline)

    //NB: Try a pattern-match to get a better error message on failures
    val Hit(Seq(pcaProjectionJob)) = pcaProjectionJobsShot.map(_.toSeq)

    val sampleClusteringJobsShot = toolbox.createJobs(pipeline.sampleClusteringTool, pipeline)

    //NB: Try a pattern-match to get a better error message on failures
    val Hit(Seq(sampleClusteringJob)) = sampleClusteringJobsShot.map(_.toSeq)

    val executable = toolbox.createExecutable(pipeline)

    val readyToExecute = false

    if (readyToExecute) {
      val jobResults = MiniExecuter.execute(executable)
      assert(jobResults.size === 4)
    }
  }

}

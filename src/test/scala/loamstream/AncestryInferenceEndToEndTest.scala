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

    val vcfFileProvider = (id: String) => FileAsker.askIfNotExist(vcfFiles.map(_ (id)))(s"VCF file '$id'")

    val pcaWeightsFileProvider = () => PcaWeightsReader.weightsFilePath(props).get

    val env = LEnv(
      Keys.vcfFilePath -> vcfFileProvider,
      Keys.pcaWeightsFilePath -> pcaWeightsFileProvider,
      Keys.klustaKwikKonfig -> KlustaKwikKonfig.withTempWorkDir("data"),
      Keys.genotypesId -> LCoreDefaultStoreIds.genotypes,
      Keys.pcaWeightsId -> LCoreDefaultStoreIds.pcaWeights)

    val genotypesId = env(Keys.genotypesId)
    val pcaWeightsId = env(Keys.pcaWeightsId)

    val pipeline = AncestryInferencePipeline(genotypesId, pcaWeightsId)
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

package loamstream

import java.nio.file.Path

import loamstream.apps.minimal.{LPipelineMiniCostEstimator, LToolMappingLogger, MiniExecuter, MiniMockToolBox}
import loamstream.map.LToolMapper
import loamstream.pipelines.qc.ancestry.AncestryInferencePipeline
import loamstream.util.FileAsker
import org.scalatest.{BeforeAndAfter, FunSuite}
import tools.PcaWeightsReader
import tools.core.LCoreEnv.{Keys, PathProvider, PathProviderById}
import tools.core.{CoreToolBox, LCoreDefaultPileIds}
import tools.klusta.KlustaKwikKonfig
import utils.Loggable.Level
import utils.{StringUtils, TestUtils}

/**
  * LoamStream
  * Created by oliverr on 4/8/2016.
  */
class AncestryInferenceEndToEndTest extends FunSuite with BeforeAndAfter {
  test("Going through ancestry inference pipeline without crashing.") {
    import TestData.sampleFiles

    val miniVcfFilePath = sampleFiles.miniVcfOpt.get
    val vcfFiles = Seq(StringUtils.pathTemplate(miniVcfFilePath.toString, "XXX"))
    val vcfFileProvider = new PathProviderById {
      override def apply(id: String): Path = FileAsker.askIfNotExist(vcfFiles.map(_ (id)))("VCF file '" + id + "'")
    }
    val pcaWeightsFileProvider = new PathProvider {
      override def get: Path = PcaWeightsReader.weightsFilePath.get
    }
    val env =
      LEnv(Keys.vcfFilePath -> vcfFileProvider, Keys.pcaWeightsFilePath -> pcaWeightsFileProvider,
        Keys.klustaKwikKonfig -> KlustaKwikKonfig.withTempWorkDir("data"),
        Keys.genotypesId -> LCoreDefaultPileIds.genotypes, Keys.pcaWeightsId -> LCoreDefaultPileIds.pcaWeights)
    val genotypesId = env(Keys.genotypesId)
    val pcaWeightsId = env(Keys.pcaWeightsId)
    val pipeline = AncestryInferencePipeline(genotypesId, pcaWeightsId)
    val toolbox = CoreToolBox(env) ++ MiniMockToolBox(env).get
    val mappings = LToolMapper.findAllSolutions(pipeline, toolbox)
    for (mapping <- mappings)
      LToolMappingLogger.logMapping(Level.trace, mapping)
    val mappingCostEstimator = LPipelineMiniCostEstimator(pipeline.genotypesId)
    val mapping = mappingCostEstimator.pickCheapest(mappings)
    LToolMappingLogger.logMapping(Level.trace, mapping)
    val pcaProjectionJob = toolbox.createJobs(pipeline.pcaProjectionRecipe, pipeline, mapping)
    assert(TestUtils.isHitOfSetOfOne(pcaProjectionJob))
    val SampleClusteringJob = toolbox.createJobs(pipeline.sampleClustering, pipeline, mapping)
    assert(TestUtils.isHitOfSetOfOne(SampleClusteringJob))
    val executable = toolbox.createExecutable(pipeline, mapping)
    val readyToExecute = false
    if (readyToExecute) {
      val jobResults = MiniExecuter.execute(executable)
      assert(jobResults.size === 4)
    }
  }

}

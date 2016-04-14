package loamstream.apps.minimal

import java.nio.file.{Files, Path}

import scala.io.Source

import org.scalatest.{BeforeAndAfter, FunSuite}

import loamstream.TestData
import loamstream.map.LToolMapper
import loamstream.util.LoamFileUtils
import loamstream.util.Loggable.Level
import loamstream.util.StringUtils
import tools.core.{CoreToolBox, LCoreDefaultPileIds, LCoreEnv}
import utils.TestUtils

/**
  * Created by kyuksel on 2/29/2016.
  */
class MiniAppEndToEndTest extends FunSuite with BeforeAndAfter {
  test("Pipeline successfully extracts sample IDs from VCF") {
    import TestData.sampleFiles

    val miniVcfFilePath = sampleFiles.miniVcfOpt.get
    val extractedSamplesFilePath = Files.createTempFile("samples", "txt")

    val vcfFiles = Seq(StringUtils.pathTemplate(miniVcfFilePath.toString, "XXX"))
    val sampleFilePaths = Seq(extractedSamplesFilePath)

    val env = LCoreEnv.FileInteractiveFallback.env(vcfFiles, sampleFilePaths, Seq.empty[Path]) +
      (LCoreEnv.Keys.genotypesId -> LCoreDefaultPileIds.genotypes)
    val genotypesId = env(LCoreEnv.Keys.genotypesId)
    val pipeline = MiniPipeline(genotypesId)
    val toolbox = CoreToolBox(env) ++ MiniMockToolBox(env).get
    val mappings = LToolMapper.findAllSolutions(pipeline, toolbox)
    for (mapping <- mappings)
      LToolMappingLogger.logMapping(Level.trace, mapping)
    val mappingCostEstimator = LPipelineMiniCostEstimator(pipeline.genotypesId)
    val mapping = mappingCostEstimator.pickCheapest(mappings)
    LToolMappingLogger.logMapping(Level.trace, mapping)

    val genotypesJob = toolbox.createJobs(pipeline.genotypeCallsRecipe, pipeline, mapping)
    assert(TestUtils.isHitOfSetOfOne(genotypesJob))
    val extractSamplesJob = toolbox.createJobs(pipeline.sampleIdsRecipe, pipeline, mapping)
    assert(TestUtils.isHitOfSetOfOne(extractSamplesJob))
    val executable = toolbox.createExecutable(pipeline, mapping)
    MiniExecuter.execute(executable)

    val source = Source.fromFile(extractedSamplesFilePath.toFile)
    LoamFileUtils.enclosed(source) { bufSrc =>
      val extractedSamplesList = bufSrc.getLines().toList
      val expectedSamplesList = List("Sample1", "Sample2", "Sample3")
      assert(extractedSamplesList == expectedSamplesList)
    }
  }
}

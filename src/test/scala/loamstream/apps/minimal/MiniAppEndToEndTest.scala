package loamstream.apps.minimal

import java.nio.file.{Files, Path}

import loamstream.TestData
import loamstream.map.LToolMapper
import org.scalatest.{BeforeAndAfter, FunSuite}
import utils.Loggable.Level
import utils.{LoamFileUtils, StringUtils, TestUtils}

import scala.io.Source

/**
  * Created by kyuksel on 2/29/2016.
  */
class MiniAppEndToEndTest extends FunSuite with BeforeAndAfter {
  test("Pipeline successfully extracts sample IDs from VCF") {
    import TestData.sampleFiles

    val miniVcfFilePath = sampleFiles.miniVcfOpt.get
    val extractedSamplesFilePath = sampleFiles.samplesOpt.get

    // Make sure to not mistakenly use an output file from a previous run, if any
    Files.deleteIfExists(extractedSamplesFilePath)

    val vcfFiles = Seq(StringUtils.pathTemplate(miniVcfFilePath.toString, "XXX"))
    val sampleFilePaths = Seq(extractedSamplesFilePath)

    val config = MiniToolBox.InteractiveFallbackConfig(vcfFiles, sampleFilePaths, Seq.empty[Path])
    val pipeline = MiniPipeline.pipeline
    val toolbox = MiniToolBox(config)
    val mappings = LToolMapper.findAllSolutions(pipeline, toolbox)
    for (mapping <- mappings)
      LToolMappingLogger.logMapping(Level.trace, mapping)
    val mappingCostEstimator = LPipelineMiniCostEstimator
    val mapping = mappingCostEstimator.pickCheapest(mappings)
    LToolMappingLogger.logMapping(Level.trace, mapping)

    val genotypesJob = toolbox.createJobs(MiniPipeline.genotypeCallsRecipe, pipeline, mapping)
    assert(TestUtils.isHitOfSetOfOne(genotypesJob))
    val extractSamplesJob = toolbox.createJobs(MiniPipeline.sampleIdsRecipe, pipeline, mapping)
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

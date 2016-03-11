package loamstream.apps.minimal

import java.nio.file.Files

import loamstream.conf.SampleFiles
import loamstream.map.LToolMapper
import loamstream.model.jobs.LJob
import loamstream.util.shot.{Hit, Shot}
import loamstream.utils.TestUtils
import org.scalatest.{BeforeAndAfter, FunSuite}
import utils.Loggable.Level
import utils.{FileUtils, StringUtils}

import scala.io.Source

/**
  * Created by kyuksel on 2/29/2016.
  */
class MiniAppEndToEndTest extends FunSuite with BeforeAndAfter {
  test("Pipeline successfully extracts sample IDs from VCF") {
    val miniVcfFilePath = TestUtils.assertSomeAndGet(SampleFiles.miniVcfOpt)
    val extractedSamplesFilePath = TestUtils.assertSomeAndGet(SampleFiles.samplesOpt)

    // Make sure to not mistakenly use an output file from a previous run, if any
    Files.deleteIfExists(extractedSamplesFilePath)

    val vcfFiles = Seq(StringUtils.pathTemplate(miniVcfFilePath.toString, "XXX"))
    val sampleFiles = Seq(extractedSamplesFilePath)

    val config = MiniToolBox.InteractiveFallbackConfig(vcfFiles, sampleFiles)
    val pipeline = MiniPipeline.pipeline
    val toolbox = MiniToolBox(config)
    val mappings = LToolMapper.findAllSolutions(pipeline, toolbox)
    for (mapping <- mappings)
      LToolMappingLogger.logMapping(Level.trace, mapping)
    val mappingCostEstimator = LPipelineMiniCostEstimator
    val mapping = mappingCostEstimator.pickCheapest(mappings)
    LToolMappingLogger.logMapping(Level.trace, mapping)

    def isHitOfSetOfOne(shot: Shot[Set[LJob]]): Boolean = {
      shot match {
        case Hit(jobs) => jobs.size == 1
        case _ => false
      }
    }

    val genotypesJob = toolbox.createJobs(MiniPipeline.genotypeCallsRecipe, pipeline, mapping)
    assert(isHitOfSetOfOne(genotypesJob))
    val extractSamplesJob = toolbox.createJobs(MiniPipeline.sampleIdsRecipe, pipeline, mapping)
    assert(isHitOfSetOfOne(extractSamplesJob))
    val executable = toolbox.createExecutable(pipeline, mapping)
    MiniExecuter.execute(executable)

    val source = Source.fromFile(extractedSamplesFilePath.toFile)
    FileUtils.enclosed(source) { bufSrc =>
      val extractedSamplesList = bufSrc.getLines.toList
      val expectedSamplesList = List("Sample1", "Sample2", "Sample3")
      assert(extractedSamplesList == expectedSamplesList)
    }
  }
}

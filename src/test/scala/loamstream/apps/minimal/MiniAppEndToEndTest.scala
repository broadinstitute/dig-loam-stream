package loamstream.apps.minimal

import java.io.File
import java.nio.file.{Files, Path, Paths}

import loamstream.map.LToolMapper
import org.scalatest.{BeforeAndAfter, FunSuite}
import utils.{FileUtils, StringUtils}

import scala.io.Source

/**
  * Created by kyuksel on 2/29/2016.
  */
class MiniAppEndToEndTest extends FunSuite with BeforeAndAfter {
  test("Pipeline successfully extracts sample IDs from VCF") {
    val miniVcfFile = "/mini.vcf"
    val extractedSamplesFile = "/samples.txt"

    def toPath(relativePath: String): Path = new File(getClass.getResource(relativePath).toURI).toPath

    val miniVcfFilePath = toPath(miniVcfFile)
    val extractedSamplesFilePath = toPath(extractedSamplesFile)

    // Make sure to not mistakenly use an output file from a previous run, if any
    Files.deleteIfExists(extractedSamplesFilePath)

    val vcfFiles = Seq(StringUtils.pathTemplate(miniVcfFilePath.toString, "XXX"))
    val sampleFiles = Seq(extractedSamplesFilePath)

    val config = MiniToolBox.InteractiveFallbackConfig(vcfFiles, sampleFiles)
    val pipeline = MiniPipeline.pipeline
    val toolbox = MiniToolBox(config)
    val mappings = LToolMapper.findAllSolutions(pipeline, toolbox)
    for (mapping <- mappings)
      LToolMappingPrinter.printMapping(mapping)
    val mappingCostEstimator = LPipelineMiniCostEstimator
    val mapping = mappingCostEstimator.pickCheapest(mappings)
    LToolMappingPrinter.printMapping(mapping)

    val genotypesJob = toolbox.createJobs(MiniPipeline.genotypeCallsRecipe, pipeline, mapping)
    val extractSamplesJob = toolbox.createJobs(MiniPipeline.sampleIdsRecipe, pipeline, mapping)
    val executable = toolbox.createExecutable(pipeline, mapping)
    MiniExecuter.execute(executable)

    FileUtils.enclosed(Source.fromFile(extractedSamplesFilePath.toFile))(bufSrc => {
      val extractedSamplesList = bufSrc.getLines().toList
      val expectedSamplesList = List("Sample1", "Sample2", "Sample3")
      assert(extractedSamplesList == expectedSamplesList)
    })
  }
}

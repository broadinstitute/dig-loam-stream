package loamstream.apps.minimal

import java.nio.file.{Files, Paths}
import loamstream.map.LToolMapper
import org.scalatest.{BeforeAndAfter, FunSuite}
import scala.io.Source
import utils.FileUtils
import utils.StringUtils

/**
  * Created by kyuksel on 2/29/2016.
  */
class MiniAppEndToEndTest extends FunSuite with BeforeAndAfter {
  test("Pipeline successfully extracts sample IDs from VCF") {
    val miniVcfFile = "/mini.vcf"
    val extractedSamplesFile = "/samples.txt"

    val miniVcfFilePath = getClass.getResource(miniVcfFile).getPath
    val extractedSamplesFilePath = getClass.getResource(extractedSamplesFile).getPath

    // Make sure to not mistakenly use an output file from a previous run, if any
    Files.deleteIfExists(Paths.get(extractedSamplesFilePath))

    val vcfFiles = Seq(StringUtils.pathTemplate(miniVcfFilePath, "XXX"))
    val sampleFiles = Seq(extractedSamplesFilePath).map(Paths.get(_))

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

    FileUtils.enclosed(Source.fromFile(extractedSamplesFilePath))(bufSrc => {
      val extractedSamplesList = bufSrc.getLines().toList
      val expectedSamplesList = List("Sample1", "Sample2", "Sample3")
      assert(extractedSamplesList == expectedSamplesList)
    })
  }
}

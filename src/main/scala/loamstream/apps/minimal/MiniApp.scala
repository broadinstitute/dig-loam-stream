package loamstream.apps.minimal

import java.nio.file.Paths

import loamstream.map.LToolMapper
import utils.{Loggable, StringUtils}

/**
  * LoamStream
  * Created by oliverr on 12/21/2015.
  */
object MiniApp extends App with Loggable {
  val vcfFiles = Seq(StringUtils.pathTemplate("dataFiles/vcf/XXX.vcf", "XXX"),
    StringUtils.pathTemplate("/home/oruebenacker/git/dig-loam-stream/dataFiles/vcf/XXX.vcf", "XXX"))
  val sampleFiles = Seq("dataFiles/samples/samples.txt",
    "/home/oruebenacker/git/dig-loam-stream/dataFiles/samples/samples.txt").map(Paths.get(_))

  val config = MiniToolBox.InteractiveFallbackConfig(vcfFiles, sampleFiles)

  debug("Yo!")

  val pipeline = MiniPipeline.pipeline

  val toolbox = MiniToolBox(config)

  val mappings = LToolMapper.findAllSolutions(pipeline, toolbox)

  debug("Found " + mappings.size + " mappings.")

  for (mapping <- mappings) {
    debug("Here comes a mapping")
    LToolMappingPrinter.printMapping(mapping)
    debug("That was a mapping")
  }

  if (mappings.isEmpty) {
    debug("No mappings found - bye")
    System.exit(0)
  }

  val mappingCostEstimator = LPipelineMiniCostEstimator

  val mapping = mappingCostEstimator.pickCheapest(mappings)

  debug("Here comes the cheapest mapping")
  LToolMappingPrinter.printMapping(mapping)
  debug("That was the cheapest mapping")

  val genotypesJob = toolbox.createJobs(MiniPipeline.genotypeCallsRecipe, pipeline, mapping)
  val extractSamplesJob = toolbox.createJobs(MiniPipeline.sampleIdsRecipe, pipeline, mapping)

  val executable = toolbox.createExecutable(pipeline, mapping)
  val results = MiniExecuter.execute(executable)
  debug(results.toString)
}

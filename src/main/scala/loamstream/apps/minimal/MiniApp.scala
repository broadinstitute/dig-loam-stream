package loamstream.apps.minimal

import loamstream.map.LToolMapper
import utils.Loggable
import utils.Loggable.Level

/**
  * LoamStream
  * Created by oliverr on 12/21/2015.
  */
object MiniApp extends App with Loggable {
  val config = MiniToolBox.InteractiveConfig

  debug("Yo!")

  val pipeline = MiniPipeline.pipeline

  val toolbox = MiniToolBox(config)

  val mappings = LToolMapper.findAllSolutions(pipeline, toolbox)

  debug("Found " + mappings.size + " mappings.")

  for (mapping <- mappings) {
    debug("Here comes a mapping")
    LToolMappingLogger.logMapping(Level.info, mapping)
    debug("That was a mapping")
  }

  if (mappings.isEmpty) {
    debug("No mappings found - bye")
    System.exit(0)
  }

  val mappingCostEstimator = LPipelineMiniCostEstimator(MiniPipeline.genotypeCallsPileId)

  val mapping = mappingCostEstimator.pickCheapest(mappings)

  debug("Here comes the cheapest mapping")
  LToolMappingLogger.logMapping(Level.info, mapping)
  debug("That was the cheapest mapping")

  val genotypesJob = toolbox.createJobs(MiniPipeline.genotypeCallsRecipe, pipeline, mapping)
  val extractSamplesJob = toolbox.createJobs(MiniPipeline.sampleIdsRecipe, pipeline, mapping)

  val executable = toolbox.createExecutable(pipeline, mapping)
  val results = MiniExecuter.execute(executable)
  debug(results.toString)
}

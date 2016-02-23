package loamstream.apps.minimal

import java.nio.file.Paths

import loamstream.map.LToolMapper

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration


/**
  * LoamStream
  * Created by oliverr on 12/21/2015.
  */
object MiniApp extends App {

  val dataFilesDir = Paths.get("C:\\Users\\oliverr\\git\\dig-loam-stream\\dataFiles")

  val config = MiniToolBox.Config(dataFilesDir)


  //  println(MiniAppDebug.theseShouldAllBeTrue())
  //  println(MiniAppDebug.theseShouldAllBeFalse())
  println("Yo!")

  val pipeline = MiniPipeline.pipeline

  val toolbox = MiniToolBox(config)

  val mappings = LToolMapper.findAllSolutions(pipeline, toolbox)

  println("Found " + mappings.size + " mappings.")

  for (mapping <- mappings) {
    println("Here comes a mapping")
    LToolMappingPrinter.printMapping(mapping)
    println("That was a mapping")
  }

  if (mappings.isEmpty) {
    println("No mappings found - bye")
    System.exit(0)
  }

  val mappingCostEstimator = LPipelineMiniCostEstimator

  val mapping = mappingCostEstimator.pickCheapest(mappings)

  println("Here comes the cheapest mapping")
  LToolMappingPrinter.printMapping(mapping)
  println("That was the cheapest mapping")

  val genotypesJob = toolbox.createJob(MiniPipeline.genotypeCallsCall.recipe, pipeline, mapping)
  val extractSamplesJob = toolbox.createJob(MiniPipeline.sampleIdsCall.recipe, pipeline, mapping)

  println(Await.result(genotypesJob.get.execute.get, Duration.Inf))
  println(Await.result(extractSamplesJob.get.execute.get, Duration.Inf))
}

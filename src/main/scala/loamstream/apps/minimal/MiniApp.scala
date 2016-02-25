package loamstream.apps.minimal

import java.nio.file.{Path, Paths}

import loamstream.map.LToolMapper


/**
  * LoamStream
  * Created by oliverr on 12/21/2015.
  */
object MiniApp extends App {

  def pathTemplate(template: String, slot: String): String => Path = {
    id => Paths.get(template.replace(slot, id))
  }

  val vcfFiles = Seq(pathTemplate("C:\\Users\\oliverr\\git\\dig-loam-stream\\dataFiles\\vcf\\XXX.vcf", "XXX"))
  val sampleFiles =
    Seq("C:\\Users\\oliverr\\git\\dig-loam-stream\\dataFiles\\samples\\samples.txt").map(Paths.get(_))

  val config = MiniToolBox.InteractiveFallbackConfig(vcfFiles, sampleFiles)


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

  val executable = toolbox.createExecutable(pipeline, mapping)
  val results = MiniExecuter.execute(executable)
  println(results)
}

package loamstream.apps.minimal

import loamstream.map.LToolMapper
import loamstream.model.jobs.LToolBox

import scala.io.Source
import java.nio.file.{Paths, Path}


/**
  * LoamStream
  * Created by oliverr on 12/21/2015.
  */
object MiniApp extends App {

  val dataFilesDir = Paths.get("C:\\Users\\oliverr\\git\\dig-loam-stream\\dataFiles")
  val vcfDirPath = dataFilesDir.resolve("vcf")

  val config = MiniToolBox.Config(vcfDirPath)


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

  if(mappings.isEmpty) {
    println("No mappings found - bye")
    System.exit(0)
  }

  val mappingCostEstimator = LPipelineMiniCostEstimator

  val mapping = mappingCostEstimator.pickCheapest(mappings)

  println("Here comes the cheapest mapping")
  LToolMappingPrinter.printMapping(mapping)
  println("That was the cheapest mapping")

}

package loamstream.apps.minimal

import loamstream.map.LToolMapper
import loamstream.map.LToolMapper.ToolMapping
import loamstream.model.jobs.LToolBox
import loamstream.model.jobs.tools.LTool
import loamstream.model.stores.LStore

/**
 * LoamStream
 * Created by oliverr on 12/21/2015.
 */
object MiniApp extends App {

  //  println(MiniAppDebug.theseShouldAllBeTrue())
  //  println(MiniAppDebug.theseShouldAllBeFalse())
  println("Yo!")

  val toolbox = LToolBox.LToolBag(MiniMockStore.stores, MiniMockTool.tools)

  val mappings = LToolMapper.findAllSolutions(MiniPipeline.pipeline, toolbox)

  for(mapping <- mappings) {
    println("Here comes a mapping")
    LToolMappingPrinter.printMapping(mapping)
    println("That was a mapping")
  }

}

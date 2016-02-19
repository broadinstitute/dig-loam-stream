package loamstream.apps.minimal

import loamstream.map.LToolMapper
import loamstream.map.LToolMapper.ToolMapping
import loamstream.model.jobs.LToolBox
import loamstream.model.jobs.tools.LTool
import loamstream.model.piles.LSig
import loamstream.model.stores.LStore

/**
  * LoamStream
  * Created by oliverr on 12/21/2015.
  */
object MiniApp extends App {

  println(MiniPipeline.genotypeCallsCall.pile.hashCode())
  println(MiniPipeline.genotypeCallsCall)
  println(MiniPipeline.sampleIdsCall)
  println(MiniMockStore.vcfFile.pile.sig =:= MiniPipeline.genotypeCallsPile.sig)
  println(MiniMockTool.checkPreExistingVcfFile.recipe <:< MiniPipeline.genotypeCallsCall.recipe)
  println(LSig.Map[(String, MiniPipeline.VariantId), MiniPipeline.GenotypeCall] ==
    LSig.Map[(String, MiniPipeline.VariantId), MiniPipeline.GenotypeCall])
  println("Yo!")

  val toolbox = LToolBox.LToolBag(MiniMockStore.stores, MiniMockTool.tools)

  val consumer = new LToolMapper.Consumer {
    def printStore(store: LStore): String = {
      store match {
        case MiniMockStore(_, comment) => comment
        case _ => store.toString
      }
    }
    def printTool(tool: LTool): String = {
      tool match {
        case MiniMockTool(_, comment) => comment
        case _ => tool.toString
      }
    }

    override def foundMapping(mapping: ToolMapping): Unit = {
      println("Yay, found a mapping!")
      for ((pile, store) <- mapping.stores) {
        println(pile + " -> " + printStore(store))
      }
      for ((recipe, tool) <- mapping.tools) {
        println(recipe + " -> " + printTool(tool))
      }
      println("That was the mapping.")
    }

    override def wantMore: Boolean = true

    override def searchEnded(): Unit = {
      println("Search ended")
    }
  }

  LToolMapper.findSolutions(MiniPipeline.pipeline, toolbox, consumer)

}

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

  println(MiniAppDebug.theseShouldAllBeTrue())
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
    def printMapping(mapping:ToolMapping) : Unit = {
      for ((pile, store) <- mapping.stores) {
        println(pile + " -> " + printStore(store))
      }
      for ((recipe, tool) <- mapping.tools) {
        println(recipe + " -> " + printTool(tool))
      }
    }

    override def intermediaryStep(mapping: ToolMapping): Unit = {
      println("Intermediary step:")
      printMapping(mapping)
      println("That was an intermediary step.")
    }

    override def foundMapping(mapping: ToolMapping): Unit = {
      println("Yay, found a mapping!")
      printMapping(mapping)
      println("That was the mapping.")
    }

    override def wantMore: Boolean = true

    override def searchEnded(): Unit = {
      println("Search ended")
    }

  }

  LToolMapper.findSolutions(MiniPipeline.pipeline, toolbox, consumer)

}

package loamstream.apps.minimal

import loamstream.map.LToolMapping
import loamstream.model.jobs.tools.LTool
import loamstream.model.stores.LStore

/**
  * LoamStream
  * Created by oliverr on 2/22/2016.
  */
object LToolMappingPrinter {
  def printStore(store: LStore): String = store.pile.id.toString

  def printTool(tool: LTool): String = {
    tool match {
      case MiniTool(_, comment) => comment
      case _ => tool.toString
    }
  }

  def printMapping(mapping: LToolMapping): Unit = {
    for ((pile, store) <- mapping.stores) {
      println(pile + " -> " + printStore(store))
    }
    for ((recipe, tool) <- mapping.tools) {
      println(recipe + " -> " + printTool(tool))
    }
  }
}

package loamstream.apps.minimal

import loamstream.map.LToolMapping
import loamstream.model.jobs.tools.LTool
import loamstream.model.stores.LStore
import utils.Loggable

/**
  * LoamStream
  * Created by oliverr on 2/22/2016.
  */
object LToolMappingPrinter extends Loggable {
  def printStore(store: LStore): String = store.id.toString

  def printTool(tool: LTool): String = tool.id.toString

  def printMapping(mapping: LToolMapping): Unit = {
    for ((pile, store) <- mapping.stores) {
      debug(pile + " -> " + printStore(store))
    }
    for ((recipe, tool) <- mapping.tools) {
      debug(recipe + " -> " + printTool(tool))
    }
  }
}

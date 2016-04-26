package loamstream.apps.minimal

import loamstream.map.LToolMapping
import loamstream.model.jobs.tools.LTool
import loamstream.util.Loggable
import loamstream.model.StoreBase

/**
  * LoamStream
  * Created by oliverr on 2/22/2016.
  */
object LToolMappingLogger extends Loggable {
  def storeToString(store: StoreBase): String = store.id.toString

  def toolToString(tool: LTool): String = tool.id.toString

  def logMapping(level: Loggable.Level.Value, mapping: LToolMapping): Unit = {
    for ((pile, store) <- mapping.stores) {
      log(level, pile + " -> " + storeToString(store))
    }
    
    for ((recipe, tool) <- mapping.tools) {
      log(level, recipe + " -> " + toolToString(tool))
    }
  }
}

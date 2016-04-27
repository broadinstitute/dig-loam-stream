package loamstream.apps.minimal

import loamstream.map.LToolMapping
import loamstream.util.Loggable
import loamstream.model.Store
import loamstream.model.ToolBase

/**
  * LoamStream
  * Created by oliverr on 2/22/2016.
  */
object LToolMappingLogger extends Loggable {
  def storeToString(store: Store): String = store.id.toString

  def toolToString(tool: ToolBase): String = tool.id.toString

  def logMapping(level: Loggable.Level.Value, mapping: LToolMapping): Unit = {
    for ((pile, store) <- mapping.stores) {
      log(level, pile + " -> " + storeToString(store))
    }
    
    for ((recipe, tool) <- mapping.tools) {
      log(level, recipe + " -> " + toolToString(tool))
    }
  }
}

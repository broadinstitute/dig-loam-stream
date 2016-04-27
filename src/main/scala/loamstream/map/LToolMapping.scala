package loamstream.map

import loamstream.model.recipes.LRecipe
import loamstream.model.Store
import loamstream.model.ToolBase

/**
  * LoamStream
  * Created by oliverr on 2/22/2016.
  */
@deprecated
case class LToolMapping(stores: Map[Store, Store], tools: Map[LRecipe, ToolBase])


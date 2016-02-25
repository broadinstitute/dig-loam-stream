package loamstream.map

import loamstream.model.jobs.tools.LTool
import loamstream.model.piles.LPile
import loamstream.model.recipes.LRecipe
import loamstream.model.stores.LStore

/**
  * LoamStream
  * Created by oliverr on 2/22/2016.
  */
case class LToolMapping(stores: Map[LPile, LStore], tools: Map[LRecipe, LTool])


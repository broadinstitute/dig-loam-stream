package loamstream.model.calls

import loamstream.model.recipes.{LPileCalls, LRecipe}
import scala.reflect.runtime.universe.{TypeTag, typeTag}

/**
 * LoamStream
 * Created by oliverr on 12/23/2015.
 */

trait LPileCall[Keys <: Product, Inputs <: LPileCalls[_, _]] {
  def keysTag: TypeTag[Keys]

  def recipe: LRecipe[Inputs]
}

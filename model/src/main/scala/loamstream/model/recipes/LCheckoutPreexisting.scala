package loamstream.model.recipes

import loamstream.model.kinds.instances.RecipeKinds
import loamstream.model.piles.LPile

/**
  * LoamStream
  * Created by oliverr on 1/5/2016.
  */
case class LCheckoutPreexisting(id: String, output: LPile) extends LRecipe {
  override val kind = RecipeKinds.usePreExisting

  override def inputs = Seq.empty[LPile]
}

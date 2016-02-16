package loamstream.model.recipes

import loamstream.model.kinds.LKind
import loamstream.model.kinds.instances.RecipeKinds
import loamstream.model.piles.LPile

import scala.language.higherKinds

/**
  * LoamStream
  * Created by oliverr on 12/23/2015.
  */
object LRecipe {

  def keyExtraction(input: LPile, output: LPile, index: Int) =
    LRecipe(RecipeKinds.extractKey(index), Seq(input), output)

  def preExistingCheckout(id: String, output: LPile) =
    LRecipe(RecipeKinds.usePreExisting(id), Seq.empty[LPile], output)

}

case class LRecipe(kind: LKind, inputs: Seq[LPile], output: LPile)

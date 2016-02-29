package loamstream.model.recipes

import loamstream.model.id.LId
import loamstream.model.kinds.LKind
import loamstream.model.kinds.instances.RecipeKinds
import loamstream.model.piles.LPile

import scala.language.higherKinds

/**
  * LoamStream
  * Created by oliverr on 12/23/2015.
  */
object LRecipe {

  def keyExtraction(input: LPile, output: LPile, index: Int): LRecipe =
    LRecipe(RecipeKinds.extractKey(index), Seq(input), output)

  def preExistingCheckout(id: String, output: LPile): LRecipe =
    LRecipe(id, RecipeKinds.usePreExisting(id), Seq.empty[LPile], output)


  def apply(id: String, kind: LKind, inputs: Seq[LPile], output: LPile): LRecipe =
    LRecipe(LId.LNamedId(id), LRecipeSpec(kind, inputs.map(_.spec), output.spec), inputs, output)

  def apply(kind: LKind, inputs: Seq[LPile], output: LPile): LRecipe =
    LRecipe(LId.newAnonId, LRecipeSpec(kind, inputs.map(_.spec), output.spec), inputs, output)

}

case class LRecipe(id: LId, spec: LRecipeSpec, inputs: Seq[LPile], output: LPile) extends LId.Owner
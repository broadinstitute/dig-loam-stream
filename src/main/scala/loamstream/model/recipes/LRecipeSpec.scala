package loamstream.model.recipes

import loamstream.model.kinds.LKind
import loamstream.model.kinds.instances.RecipeKinds
import loamstream.model.piles.LPile

import scala.language.higherKinds

/**
  * LoamStream
  * Created by oliverr on 02/26/2016.
  */
object LRecipeSpec {

  def keyExtraction(input: LPile, output: LPile, index: Int) =
    LRecipeSpec(RecipeKinds.extractKey(index), Seq(input), output)

  def preExistingCheckout(id: String, output: LPile) =
    LRecipeSpec(RecipeKinds.usePreExisting(id), Seq.empty[LPile], output)

}

case class LRecipeSpec(kind: LKind, inputs: Seq[LPile], output: LPile) {
  def =:=(oRecipe: LRecipeSpec): Boolean =
    kind == oRecipe.kind && inputs.zip(oRecipe.inputs).forall(tup => tup._1.spec =:= tup._2.spec)

  def <:<(oRecipe: LRecipeSpec): Boolean = kind <:< oRecipe.kind && inputs.size == oRecipe.inputs.size &&
    inputs.zip(oRecipe.inputs).forall(tup => tup._1.spec >:> tup._2.spec) && output.spec <:< oRecipe.output.spec

  def >:>(oRecipe: LRecipeSpec): Boolean = kind >:> oRecipe.kind && inputs.size == oRecipe.inputs.size &&
    inputs.zip(oRecipe.inputs).forall(tup => tup._1.spec <:< tup._2.spec) && output.spec >:> oRecipe.output.spec

  def <<<(oRecipe: LRecipeSpec): Boolean = kind <:< oRecipe.kind && inputs.size == oRecipe.inputs.size &&
    inputs.zip(oRecipe.inputs).forall(tup => tup._1.spec <:< tup._2.spec) && output.spec <:< oRecipe.output.spec

  def >>>(oRecipe: LRecipeSpec): Boolean = kind >:> oRecipe.kind && inputs.size == oRecipe.inputs.size &&
    inputs.zip(oRecipe.inputs).forall(tup => tup._1.spec >:> tup._2.spec) && output.spec >:> oRecipe.output.spec

}

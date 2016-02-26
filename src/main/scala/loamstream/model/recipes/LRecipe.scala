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

  def keyExtraction(input: LPile, output: LPile, index: Int) = {
    val kind = RecipeKinds.extractKey(index)
    val inputs = Seq(input)
    LRecipe(LRecipeSpec(kind, inputs, output), kind, inputs, output)
  }

  def preExistingCheckout(id: String, output: LPile) = {
    val kind = RecipeKinds.usePreExisting(id)
    val inputs = Seq.empty[LPile]
    LRecipe(LRecipeSpec(kind, inputs, output), kind, inputs, output)
  }

}

case class LRecipe(spec: LRecipeSpec, kind: LKind, inputs: Seq[LPile], output: LPile) {
  def =:=(oRecipe: LRecipe): Boolean =
    kind == oRecipe.kind && inputs.zip(oRecipe.inputs).forall(tup => tup._1.spec =:= tup._2.spec)

  def <:<(oRecipe: LRecipe): Boolean = kind <:< oRecipe.kind && inputs.size == oRecipe.inputs.size &&
    inputs.zip(oRecipe.inputs).forall(tup => tup._1.spec >:> tup._2.spec) && output.spec <:< oRecipe.output.spec

  def >:>(oRecipe: LRecipe): Boolean = kind >:> oRecipe.kind && inputs.size == oRecipe.inputs.size &&
    inputs.zip(oRecipe.inputs).forall(tup => tup._1.spec <:< tup._2.spec) && output.spec >:> oRecipe.output.spec

  def <<<(oRecipe: LRecipe): Boolean = kind <:< oRecipe.kind && inputs.size == oRecipe.inputs.size &&
    inputs.zip(oRecipe.inputs).forall(tup => tup._1.spec <:< tup._2.spec) && output.spec <:< oRecipe.output.spec

  def >>>(oRecipe: LRecipe): Boolean = kind >:> oRecipe.kind && inputs.size == oRecipe.inputs.size &&
    inputs.zip(oRecipe.inputs).forall(tup => tup._1.spec >:> tup._2.spec) && output.spec >:> oRecipe.output.spec

}

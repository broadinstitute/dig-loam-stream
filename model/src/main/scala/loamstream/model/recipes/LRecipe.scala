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

case class LRecipe(kind: LKind, inputs: Seq[LPile], output: LPile) {
  def =:=(oRecipe: LRecipe): Boolean =
    kind == oRecipe.kind && inputs.zip(oRecipe.inputs).forall(tup => tup._1 =:= tup._2)

  def <:<(oRecipe: LRecipe): Boolean = kind <:< oRecipe.kind && inputs.size == oRecipe.inputs.size &&
    inputs.zip(oRecipe.inputs).forall(tup => tup._1 >:> tup._2) && output <:< oRecipe.output

  def >:>(oRecipe: LRecipe): Boolean = kind >:> oRecipe.kind && inputs.size == oRecipe.inputs.size &&
    inputs.zip(oRecipe.inputs).forall(tup => tup._1 <:< tup._2) && output >:> oRecipe.output

  def <<<(oRecipe: LRecipe): Boolean = kind <:< oRecipe.kind && inputs.size == oRecipe.inputs.size &&
    inputs.zip(oRecipe.inputs).forall(tup => tup._1 <:< tup._2) && output <:< oRecipe.output

  def >>>(oRecipe: LRecipe): Boolean = kind >:> oRecipe.kind && inputs.size == oRecipe.inputs.size &&
    inputs.zip(oRecipe.inputs).forall(tup => tup._1 >:> tup._2) && output >:> oRecipe.output

}

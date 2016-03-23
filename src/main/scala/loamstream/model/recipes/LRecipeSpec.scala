package loamstream.model.recipes

import loamstream.model.kinds.LKind
import loamstream.model.kinds.instances.RecipeKinds
import loamstream.model.piles.LPileSpec

import scala.language.higherKinds

/**
  * LoamStream
  * Created by oliverr on 02/26/2016.
  */
object LRecipeSpec {

  def keyExtraction(input: LPileSpec, output: LPileSpec, index: Int): LRecipeSpec =
    LRecipeSpec(RecipeKinds.extractKey(index), Seq(input), output)

  def vcfImport(input: LPileSpec, output: LPileSpec, index: Int): LRecipeSpec =
    LRecipeSpec(RecipeKinds.importVcf(index), Seq(input), output)

  def calculateSingletons(input: LPileSpec, output: LPileSpec, index: Int): LRecipeSpec =
    LRecipeSpec(RecipeKinds.calculateSingletons(index), Seq(input), output)

  def preExistingCheckout(id: String, output: LPileSpec): LRecipeSpec =
    LRecipeSpec(RecipeKinds.usePreExisting(id), Seq.empty[LPileSpec], output)

}

case class LRecipeSpec(kind: LKind, inputs: Seq[LPileSpec], output: LPileSpec) {
  def =:=(oRecipe: LRecipeSpec): Boolean =
    kind == oRecipe.kind && inputs.zip(oRecipe.inputs).forall(tup => tup._1 =:= tup._2)

  def <:<(oRecipe: LRecipeSpec): Boolean = kind <:< oRecipe.kind && inputs.size == oRecipe.inputs.size &&
    inputs.zip(oRecipe.inputs).forall(tup => tup._1 >:> tup._2) && output <:< oRecipe.output

  def >:>(oRecipe: LRecipeSpec): Boolean = kind >:> oRecipe.kind && inputs.size == oRecipe.inputs.size &&
    inputs.zip(oRecipe.inputs).forall(tup => tup._1 <:< tup._2) && output >:> oRecipe.output

  def <<<(oRecipe: LRecipeSpec): Boolean = kind <:< oRecipe.kind && inputs.size == oRecipe.inputs.size &&
      inputs.zip(oRecipe.inputs).forall(tup => tup._1 <:< tup._2) && output <:< oRecipe.output

  def >>>(oRecipe: LRecipeSpec): Boolean = kind >:> oRecipe.kind && inputs.size == oRecipe.inputs.size &&
    inputs.zip(oRecipe.inputs).forall(tup => tup._1 >:> tup._2) && output >:> oRecipe.output

}

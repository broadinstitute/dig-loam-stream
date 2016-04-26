package loamstream.model.recipes

import loamstream.model.id.LId
import loamstream.model.kinds.LKind
import loamstream.model.kinds.instances.RecipeKinds

import scala.language.higherKinds
import loamstream.model.StoreBase

/**
  * LoamStream
  * Created by oliverr on 12/23/2015.
  */
object LRecipe {

  def keyExtraction(input: StoreBase, output: StoreBase, index: Int): LRecipe = {
    LRecipe(RecipeKinds.extractKey(index), Seq(input), output)
  }

  def preExistingCheckout(id: String, output: StoreBase): LRecipe = {
    LRecipe(id, RecipeKinds.usePreExisting(id), Seq.empty[StoreBase], output)
  }

  def vcfImport(input: StoreBase, output: StoreBase, index: Int): LRecipe = {
    LRecipe(RecipeKinds.importVcf(index), Seq(input), output)
  }

  def singletonCalculation(input: StoreBase, output: StoreBase, index: Int): LRecipe = {
    LRecipe(RecipeKinds.calculateSingletons(index), Seq(input), output)
  }

  def apply(id: String, kind: LKind, inputs: Seq[StoreBase], output: StoreBase): LRecipe = {
    LRecipe(LId.LNamedId(id), LRecipeSpec(kind, inputs.map(_.spec), output.spec), inputs, output)
  }

  def apply(kind: LKind, inputs: Seq[StoreBase], output: StoreBase): LRecipe = {
    LRecipe(LId.newAnonId, LRecipeSpec(kind, inputs.map(_.spec), output.spec), inputs, output)
  }
}

case class LRecipe(id: LId, spec: LRecipeSpec, inputs: Seq[StoreBase], output: StoreBase) extends LId.Owner
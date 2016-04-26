package loamstream.model.recipes

import loamstream.model.id.LId
import loamstream.model.kinds.LKind
import loamstream.model.kinds.instances.RecipeKinds

import scala.language.higherKinds
import loamstream.model.Store

/**
  * LoamStream
  * Created by oliverr on 12/23/2015.
  */
object LRecipe {

  def keyExtraction(input: Store, output: Store, index: Int): LRecipe = {
    LRecipe(RecipeKinds.extractKey(index), Seq(input), output)
  }

  def preExistingCheckout(id: String, output: Store): LRecipe = {
    LRecipe(id, RecipeKinds.usePreExisting(id), Seq.empty[Store], output)
  }

  def vcfImport(input: Store, output: Store, index: Int): LRecipe = {
    LRecipe(RecipeKinds.importVcf(index), Seq(input), output)
  }

  def singletonCalculation(input: Store, output: Store, index: Int): LRecipe = {
    LRecipe(RecipeKinds.calculateSingletons(index), Seq(input), output)
  }

  def apply(id: String, kind: LKind, inputs: Seq[Store], output: Store): LRecipe = {
    LRecipe(LId.LNamedId(id), LRecipeSpec(kind, inputs.map(_.spec), output.spec), inputs, output)
  }

  def apply(kind: LKind, inputs: Seq[Store], output: Store): LRecipe = {
    LRecipe(LId.newAnonId, LRecipeSpec(kind, inputs.map(_.spec), output.spec), inputs, output)
  }
}

case class LRecipe(id: LId, spec: LRecipeSpec, inputs: Seq[Store], output: Store) extends LId.Owner
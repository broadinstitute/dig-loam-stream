package loamstream.model

import loamstream.model.id.LId
import loamstream.model.recipes.LRecipeSpec
import loamstream.model.recipes.LRecipe
import loamstream.model.kinds.LKind
import loamstream.model.kinds.instances.RecipeKinds

/**
 * @author clint
 * date: Apr 26, 2016
 */
trait ToolBase extends LId.Owner {
  def spec: LRecipeSpec 
  
  def inputs: Seq[Store] 
  
  def output: Store
}

object ToolBase {

  def keyExtraction(input: Store, output: Store, index: Int): ToolBase = {
    LRecipe(RecipeKinds.extractKey(index), Seq(input), output)
  }

  def preExistingCheckout(id: String, output: Store): ToolBase = {
    LRecipe(id, RecipeKinds.usePreExisting(id), Seq.empty[Store], output)
  }

  def vcfImport(input: Store, output: Store, index: Int): ToolBase = {
    LRecipe(RecipeKinds.importVcf(index), Seq(input), output)
  }

  def singletonCalculation(input: Store, output: Store, index: Int): ToolBase = {
    LRecipe(RecipeKinds.calculateSingletons(index), Seq(input), output)
  }

  def apply(id: String, kind: LKind, inputs: Seq[Store], output: Store): ToolBase = {
    LRecipe(LId.LNamedId(id), LRecipeSpec(kind, inputs.map(_.spec), output.spec), inputs, output)
  }

  def apply(kind: LKind, inputs: Seq[Store], output: Store): ToolBase = {
    LRecipe(LId.newAnonId, LRecipeSpec(kind, inputs.map(_.spec), output.spec), inputs, output)
  }
}
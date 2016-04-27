package loamstream.model

import loamstream.model.id.LId
import loamstream.model.recipes.LRecipeSpec
import loamstream.model.kinds.LKind
import loamstream.model.kinds.instances.RecipeKinds
import loamstream.tools.core.CoreTool
import loamstream.tools.core.StoreOps

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

  import StoreOps._

  def apply(id: String, kind: LKind, inputs: Seq[Store], output: Store): ToolBase = {
    CoreTool.nAryTool(id, kind, inputs ~> output)
  }

  def apply(kind: LKind, inputs: Seq[Store], output: Store): ToolBase = {
    CoreTool.nAryTool(kind, inputs ~> output)
  }
  
  def keyExtraction(input: Store, output: Store, index: Int): ToolBase = {
    //TODO: CoreTool.unaryTool() overload?
    ToolBase(RecipeKinds.extractKey(index), Seq(input), output)
  }

  def preExistingCheckout(id: String, output: Store): ToolBase = {
    //TODO: CoreTool.nullaryTool() overload?
    ToolBase(id, RecipeKinds.usePreExisting(id), Seq.empty[Store], output)
  }

  def vcfImport(input: Store, output: Store, index: Int): ToolBase = {
    ToolBase(RecipeKinds.importVcf(index), Seq(input), output)
  }

  def singletonCalculation(input: Store, output: Store, index: Int): ToolBase = {
    ToolBase(RecipeKinds.calculateSingletons(index), Seq(input), output)
  }
}
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
trait Tool extends LId.Owner {
  def spec: LRecipeSpec 
  
  def inputs: Seq[Store] 
  
  def output: Store
}

object Tool {

  import StoreOps._

  def apply(id: String, kind: LKind, inputs: Seq[Store], output: Store): Tool = {
    CoreTool.nAryTool(id, kind, inputs ~> output)
  }

  def apply(kind: LKind, inputs: Seq[Store], output: Store): Tool = {
    CoreTool.nAryTool(kind, inputs ~> output)
  }
  
  def keyExtraction(input: Store, output: Store, index: Int): Tool = {
    //TODO: CoreTool.unaryTool() overload?
    Tool(RecipeKinds.extractKey(index), Seq(input), output)
  }

  def preExistingCheckout(id: String, output: Store): Tool = {
    //TODO: CoreTool.nullaryTool() overload?
    Tool(id, RecipeKinds.usePreExisting(id), Seq.empty[Store], output)
  }

  def vcfImport(input: Store, output: Store, index: Int): Tool = {
    Tool(RecipeKinds.importVcf(index), Seq(input), output)
  }

  def singletonCalculation(input: Store, output: Store, index: Int): Tool = {
    Tool(RecipeKinds.calculateSingletons(index), Seq(input), output)
  }
}
package loamstream.model

import loamstream.model.kinds.LKind
import loamstream.model.kinds.ToolKinds
import loamstream.tools.core.CoreTool
import loamstream.tools.core.StoreOps

/**
 * @author Clint
 * @author Oliver
 * date: Apr 26, 2016
 */
trait Tool extends LId.Owner {
  def spec: ToolSpec 
  
  def inputs: Seq[StoreSpec] 
  
  def output: StoreSpec
}

object Tool {

  import StoreOps._

  def apply(id: String, kind: LKind, inputs: Seq[StoreSpec], output: StoreSpec): Tool = {
    CoreTool.nAryTool(id, kind, inputs ~> output)
  }

  def apply(kind: LKind, inputs: Seq[StoreSpec], output: StoreSpec): Tool = {
    CoreTool.nAryTool(kind, inputs ~> output)
  }
  
  def keyExtraction(input: StoreSpec, output: StoreSpec, index: Int): Tool = {
    //TODO: CoreTool.unaryTool() overload?
    Tool(ToolKinds.extractKey(index), Seq(input), output)
  }

  def preExistingCheckout(id: String, output: StoreSpec): Tool = {
    //TODO: CoreTool.nullaryTool() overload?
    Tool(id, ToolKinds.usePreExisting(id), Seq.empty[StoreSpec], output)
  }

  def vcfImport(input: StoreSpec, output: StoreSpec, index: Int): Tool = {
    Tool(ToolKinds.importVcf(index), Seq(input), output)
  }

  def singletonCalculation(input: StoreSpec, output: StoreSpec, index: Int): Tool = {
    Tool(ToolKinds.calculateSingletons(index), Seq(input), output)
  }
}
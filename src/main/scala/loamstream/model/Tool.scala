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
  
  def inputs: Map[LId, Store] 
  
  def outputs: Map[LId, Store]
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
    Tool(ToolKinds.extractKey(index), Seq(input), output)
  }

  def preExistingCheckout(id: String, output: Store): Tool = {
    //TODO: CoreTool.nullaryTool() overload?
    Tool(id, ToolKinds.usePreExisting(id), Seq.empty[Store], output)
  }

  def vcfImport(input: Store, output: Store, index: Int): Tool = {
    Tool(ToolKinds.importVcf(index), Seq(input), output)
  }

  def singletonCalculation(input: Store, output: Store, index: Int): Tool = {
    Tool(ToolKinds.calculateSingletons(index), Seq(input), output)
  }
}
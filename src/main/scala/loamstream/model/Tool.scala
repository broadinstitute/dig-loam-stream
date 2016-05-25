package loamstream.model

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

  trait NoInputs { self: Tool =>
    override def inputs: Map[LId, Store] = Map.empty
  }
  
  import StoreOps._

  def apply(id: String, inputs: Seq[Store], output: Store): Tool = {
    CoreTool.nAryTool(id, inputs ~> output)
  }

  def apply(inputs: Seq[Store], output: Store): Tool = {
    CoreTool.nAryTool(inputs ~> output)
  }
  
  def keyExtraction(input: Store, output: Store, index: Int): Tool = {
    //TODO: CoreTool.unaryTool() overload?
    Tool(Seq(input), output)
  }

  def preExistingCheckout(id: String, output: Store): Tool = {
    //TODO: CoreTool.nullaryTool() overload?
    Tool(id, Seq.empty[Store], output)
  }

  def vcfImport(input: Store, output: Store, index: Int): Tool = {
    Tool(Seq(input), output)
  }

  def singletonCalculation(input: Store, output: Store, index: Int): Tool = {
    Tool(Seq(input), output)
  }
}
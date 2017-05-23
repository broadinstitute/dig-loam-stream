package loamstream.loam.ops

import loamstream.model.{Store, Tool}
import loamstream.model.Tool.InputsAndOutputs

/** A tool based on a store op */
trait LoamStoreOpTool[SI <: StoreType, SO <: StoreType] extends Tool {

  scriptContext.projectContext.graphBox.mutate(_.withTool(this, scriptContext))

  /** The store op this tool is based on */
  def op: LoamStoreOp[SI, SO]

  /** The input store */
  def inStore: Store[SI]

  /** The output store */
  def outStore: Store[SO]

  /** Input and output stores before any are specified using in or out */
  override def defaultStores: InputsAndOutputs = InputsAndOutputs(Tool.In(inStore), Tool.Out(outStore))

}

package loamstream.loam.ops

import loamstream.loam.LoamTool.InputsAndOutputs
import loamstream.loam.LoamTool
import loamstream.model.Store

/** A tool based on a store op */
trait LoamStoreOpTool[SI <: StoreType, SO <: StoreType] extends LoamTool {

  scriptContext.projectContext.graphBox.mutate(_.withTool(this, scriptContext))

  /** The store op this tool is based on */
  def op: LoamStoreOp[SI, SO]

  /** The input store */
  def inStore: Store[SI]

  /** The output store */
  def outStore: Store[SO]

  /** Input and output stores before any are specified using in or out */
  override def defaultStores: InputsAndOutputs = InputsAndOutputs(LoamTool.In(inStore), LoamTool.Out(outStore))

}

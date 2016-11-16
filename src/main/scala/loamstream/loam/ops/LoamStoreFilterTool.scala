package loamstream.loam.ops

import loamstream.loam.LoamTool.InputsAndOutputs
import loamstream.loam.{LoamScriptContext, LoamStore, LoamTool}
import loamstream.model.LId

/** A tool based on store ops */
final case class LoamStoreFilterTool[In, Out](id: LId, filter: LoamStoreFilter[In, Out],
                                              inStore: LoamStore[In], outStore: LoamStore[Out])(
                                               implicit val scriptContext: LoamScriptContext) extends LoamTool {
  /** Input and output stores before any are specified using in or out */
  override def defaultStores: InputsAndOutputs = InputsAndOutputs(LoamTool.In(inStore), LoamTool.Out(outStore))
}

/** A tool based on store ops */
object LoamStoreFilterTool {
  def apply[In, Out](filter: LoamStoreFilter[In, Out],
                     inStore: LoamStore[In], outStore: LoamStore[Out])(
                      implicit scriptContext: LoamScriptContext): LoamStoreFilterTool[In, Out]
  = LoamStoreFilterTool(LId.newAnonId, filter, inStore, outStore)
}

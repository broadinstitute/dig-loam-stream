package loamstream.loam.ops.filters

import loamstream.loam.LoamTool.InputsAndOutputs
import loamstream.loam.{LoamScriptContext, LoamStore, LoamTool}
import loamstream.model.LId

/** A tool based on store ops */
final case class LoamStoreFilterTool[Store](id: LId, filter: LoamStoreFilter[Store],
                                            inStore: LoamStore[Store], outStore: LoamStore[Store])(
                                             implicit val scriptContext: LoamScriptContext) extends LoamTool {
  /** Input and output stores before any are specified using in or out */
  override def defaultStores: InputsAndOutputs = InputsAndOutputs(LoamTool.In(inStore), LoamTool.Out(outStore))
}

/** A tool based on store ops */
object LoamStoreFilterTool {
  def apply[Store](filter: LoamStoreFilter[Store],
                   inStore: LoamStore[Store], outStore: LoamStore[Store])(
                    implicit scriptContext: LoamScriptContext): LoamStoreFilterTool[Store]
  = LoamStoreFilterTool(LId.newAnonId, filter, inStore, outStore)
}

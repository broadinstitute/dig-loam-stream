package loamstream.loam.ops.filters

import loamstream.loam.LoamTool.InputsAndOutputs
import loamstream.loam.{LoamScriptContext, LoamStore, LoamTool}
import loamstream.model.LId

/** A tool based on store ops */
final case class LoamStoreFilterTool[Store, Record](id: LId, filter: LoamStoreFilter[Store, Record],
                                                    inStore: LoamStore[Store], outStore: LoamStore[Store])(
                                                     implicit val scriptContext: LoamScriptContext) extends LoamTool {
  /** Input and output stores before any are specified using in or out */
  override def defaultStores: InputsAndOutputs = InputsAndOutputs(LoamTool.In(inStore), LoamTool.Out(outStore))
}

/** A tool based on store ops */
object LoamStoreFilterTool {
  def apply[Store, Record](filter: LoamStoreFilter[Store, Record],
                           inStore: LoamStore[Store], outStore: LoamStore[Store])(
                            implicit scriptContext: LoamScriptContext): LoamStoreFilterTool[Store, Record]
  = LoamStoreFilterTool(LId.newAnonId, filter, inStore, outStore)
}

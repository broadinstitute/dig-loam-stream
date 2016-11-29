package loamstream.loam.ops.filters

import loamstream.loam.ops.{LoamStoreOpTool, StoreType}
import loamstream.loam.{LoamScriptContext, LoamStore}
import loamstream.model.LId

/** A tool based on a store filter */
final case class LoamStoreFilterTool[S <: StoreType](id: LId,
                                                     filter: LoamStoreFilter[S],
                                                     inStore: LoamStore[S],
                                                     outStore: LoamStore[S])(
                                                      implicit val scriptContext: LoamScriptContext)
  extends LoamStoreOpTool[S, S] {
  def op: LoamStoreFilter[S] = filter
}

/** A tool based on a store filter */
object LoamStoreFilterTool {
  def apply[S <: StoreType](filter: LoamStoreFilter[S],
                            inStore: LoamStore[S],
                            outStore: LoamStore[S])(
                             implicit scriptContext: LoamScriptContext): LoamStoreFilterTool[S] = {

    LoamStoreFilterTool(LId.newAnonId, filter, inStore, outStore)
  }
}

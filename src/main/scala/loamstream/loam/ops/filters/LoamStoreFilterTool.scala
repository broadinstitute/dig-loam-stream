package loamstream.loam.ops.filters

import loamstream.loam.LoamScriptContext
import loamstream.loam.ops.{LoamStoreOpTool, StoreType}
import loamstream.model.{LId, Store}

/** A tool based on a store filter */
final case class LoamStoreFilterTool[S <: StoreType](
    id: LId,
    filter: LoamStoreFilter[S],
    inStore: Store[S],
    outStore: Store[S])(implicit val scriptContext: LoamScriptContext) extends LoamStoreOpTool[S, S] {

  override def op: LoamStoreFilter[S] = filter

}

/** A tool based on a store filter */
object LoamStoreFilterTool {
  def apply[S <: StoreType](
      filter: LoamStoreFilter[S],
      inStore: Store[S],
      outStore: Store[S])(implicit scriptContext: LoamScriptContext): LoamStoreFilterTool[S] = {

    LoamStoreFilterTool(LId.newAnonId, filter, inStore, outStore)
  }
}

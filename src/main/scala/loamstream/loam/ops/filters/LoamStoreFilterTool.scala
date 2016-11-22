package loamstream.loam.ops.filters

import loamstream.loam.LoamTool.InputsAndOutputs
import loamstream.loam.ops.StoreType
import loamstream.loam.{LoamScriptContext, LoamStore, LoamTool}
import loamstream.model.LId

/** A tool based on store ops */
final case class LoamStoreFilterTool[S <: StoreType](
    id: LId, 
    filter: LoamStoreFilter[S],
    inStore: LoamStore[S], 
    outStore: LoamStore[S])(implicit val scriptContext: LoamScriptContext) extends LoamTool {

  scriptContext.projectContext.graphBox.mutate(_.withTool(this, scriptContext))

  /** Input and output stores before any are specified using in or out */
  override def defaultStores: InputsAndOutputs = InputsAndOutputs(LoamTool.In(inStore), LoamTool.Out(outStore))
}

/** A tool based on store ops */
object LoamStoreFilterTool {
  def apply[S <: StoreType](
      filter: LoamStoreFilter[S], 
      inStore: LoamStore[S], 
      outStore: LoamStore[S])(implicit scriptContext: LoamScriptContext): LoamStoreFilterTool[S] = {

    LoamStoreFilterTool(LId.newAnonId, filter, inStore, outStore)
  }
}

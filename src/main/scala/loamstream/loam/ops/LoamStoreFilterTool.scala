package loamstream.loam.ops

import loamstream.loam.LoamTool.DefaultStores
import loamstream.loam.{LoamScriptContext, LoamTool}
import loamstream.model.LId

/** A tool based on store ops */
final case class LoamStoreFilterTool[In, Out](id: LId, filter: LoamStoreFilter[In, Out], defaultStores: DefaultStores)(
  implicit val scriptContext: LoamScriptContext) extends LoamTool {
}

package loamstream.loam.ops

import loamstream.loam.{LoamScriptContext, LoamStore}

/** A filter to be applied to records of a store */
trait LoamStoreFilter[In, Out] {

  /** A new suitable output store */
  def newOutStore: LoamStore[Out]

  /** A new suitable Loam store filter tool */
  def newTool(inStore: LoamStore[In])(implicit scriptContext: LoamScriptContext): LoamStoreFilterTool[In, Out] =
  LoamStoreFilterTool(this, inStore, newOutStore)

}

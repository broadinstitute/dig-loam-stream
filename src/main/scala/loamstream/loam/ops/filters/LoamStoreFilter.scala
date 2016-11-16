package loamstream.loam.ops.filters

import loamstream.loam.{LoamScriptContext, LoamStore}

/** A filter to be applied to records of a store */
trait LoamStoreFilter[Store] {

  type Record

  /** Test a record */
  def test(record: Record): Boolean

  /** A new suitable output store */
  def newOutStore(inStore: LoamStore[Store])(implicit scriptContext: LoamScriptContext): LoamStore[Store] =
  LoamStore.createOfType[Store](inStore.sig.tpe)

  /** A new suitable Loam store filter tool */
  def newTool(inStore: LoamStore[Store])(implicit scriptContext: LoamScriptContext): LoamStoreFilterTool[Store] =
  LoamStoreFilterTool(this, inStore, newOutStore(inStore))

}

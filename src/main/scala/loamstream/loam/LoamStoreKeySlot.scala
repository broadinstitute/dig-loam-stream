package loamstream.loam

import loamstream.model.Store

/** A key slot of a Loam store, defined by store and slot name */
final case class LoamStoreKeySlot(store: Store.Untyped, name: String)(implicit context: LoamProjectContext) {
  /** Specifies that this key slot has the same set of keys as that key slot (order may be different) */
  def setSameSetAs(oSlot: LoamStoreKeySlot): LoamStoreKeySlot = {
    context.updateGraph(_.withKeysSameSet(this, oSlot))
    this
  }

  /** Specifies that this key slot has the same list of keys as that key slot (same order) */
  def setSameListAs(oSlot: LoamStoreKeySlot): LoamStoreKeySlot = {
    context.updateGraph(_.withKeysSameList(this, oSlot))
    this
  }

  /** True if this and that slot have same same key set */
  def isSameSetAs(oSlot: LoamStoreKeySlot): Boolean = context.graph.areSameKeySets(this, oSlot)

  /** True if this and that slot have same same key list */
  def isSameListAs(oSlot: LoamStoreKeySlot): Boolean = context.graph.areSameKeyLists(this, oSlot)
}

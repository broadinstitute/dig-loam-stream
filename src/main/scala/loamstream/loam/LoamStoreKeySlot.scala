package loamstream.loam

import loamstream.util.ValueBox

/** A key slot of a Loam store, defined by store and slot name */
final case class LoamStoreKeySlot(store: LoamStore, name: String)(implicit graphBox: ValueBox[LoamGraph]) {
  /** Specifies that this key slot has the same set of keys as that key slot (order may be different) */
  def sameSetAs(oSlot: LoamStoreKeySlot): LoamStoreKeySlot = {
    graphBox(_.withKeysSameSet(this, oSlot))
    this
  }

  /** Specifies that this key slot has the same list of keys as that key slot (same order) */
  def sameListAs(oSlot: LoamStoreKeySlot): LoamStoreKeySlot = {
    graphBox(_.withKeysSameList(this, oSlot))
    this
  }
}

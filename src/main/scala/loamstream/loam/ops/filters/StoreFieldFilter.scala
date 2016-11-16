package loamstream.loam.ops.filters

import loamstream.loam.ops.StoreField

/** A LoamStoreFilter based on a given field */
class StoreFieldFilter[Store, Value](field: StoreField[Value],
                                     valueFilter: Value => Boolean) extends LoamStoreFilter[Store] {
  override type Record

  /** Test a record */
  override def test(record: Record): Boolean = ???
}

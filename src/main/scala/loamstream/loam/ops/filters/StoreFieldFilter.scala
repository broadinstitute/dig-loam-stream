package loamstream.loam.ops.filters

import loamstream.loam.ops.{StoreField, StoreRecord}

/** A LoamStoreFilter based on a given field */
class StoreFieldFilter[Store, Record <: StoreRecord, Value](field: StoreField[Store, Record, Value],
                                                            valueFilter: Value => Boolean)
  extends LoamStoreFilter[Store, Record] {
  /** Test a record */
  override def test(record: Record): Boolean = field.get(record).exists(valueFilter)
}

package loamstream.loam.ops.filters

import loamstream.loam.ops.{StoreField, StoreType}

/** A LoamStoreFilter based on a given field */
class StoreFieldFilter[Store <: StoreType, Value](field: StoreField[Store, Value], valueFilter: Value => Boolean)
  extends LoamStoreFilter[Store] {
  /** Test a record */
  override def test(record: Store#Record): Boolean = field.get(record).exists(valueFilter)
}

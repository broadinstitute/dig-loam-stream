package loamstream.loam.ops.filters

import loamstream.loam.ops.{StoreField, StoreType}

/** A LoamStoreFilter based on a given field */
case class StoreFieldValueFilter[Store <: StoreType, Value](field: StoreField[Store, Value],
                                                            valueFilter: Value => Boolean)
  extends StoreFieldFilter[Store, Value] {
  /** Test a record */
  override def test(record: Store#Record): Boolean = field.get(record).exists(valueFilter)
}


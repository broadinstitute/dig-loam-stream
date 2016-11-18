package loamstream.loam.ops.filters

import loamstream.loam.ops.{StoreField, StoreType}

import scala.reflect.runtime.universe.{Type, TypeTag, typeTag}


/** A LoamStoreFilter based on a given field */
case class StoreFieldValueFilter[Store <: StoreType : TypeTag, Value](field: StoreField[Store, Value],
                                                                      valueFilter: Value => Boolean)
  extends StoreFieldFilter[Store, Value] {

  override val tpe: Type = typeTag[Store].tpe

  /** Test a record */
  override def test(record: Store#Record): Boolean = field.get(record).exists(valueFilter)
}


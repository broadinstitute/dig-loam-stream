package loamstream.loam.ops.filters

import loamstream.loam.ops.{StoreField, StoreType}

import scala.reflect.runtime.universe.{Type, TypeTag, typeTag}


/** A LoamStoreFilter based on a given field */
final case class StoreFieldValueFilter[S <: StoreType : TypeTag, V](
    field: StoreField[S, V], 
    valueFilter: V => Boolean) extends StoreFieldFilter[S, V] {

  override val tpe: Type = typeTag[S].tpe

  /** Test a record */
  override def test(record: S#Record): Boolean = field.get(record).exists(valueFilter)
}


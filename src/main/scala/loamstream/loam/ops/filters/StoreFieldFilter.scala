package loamstream.loam.ops.filters

import loamstream.loam.ops.{StoreField, StoreType}

/**
  * LoamStream
  * Created by oliverr on 11/17/2016.
  */
trait StoreFieldFilter[Store <: StoreType, Value]
  extends LoamStoreFilter[Store] {

  def field: StoreField[Store, Value]

  /** Test a record */
  override def test(record: Store#Record): Boolean
}

object StoreFieldFilter {

  case class IsDefinedFilter[Store <: StoreType, Value](field: StoreField[Store, Value])
    extends StoreFieldFilter[Store, Value] {

    /** Test a record */
    override def test(record: Store#Record): Boolean = field.get(record).nonEmpty
  }

  def isDefined[Store <: StoreType, Value](field: StoreField[Store, Value]):
  StoreFieldFilter.IsDefinedFilter[Store, Value]
  = IsDefinedFilter(field)

  case class IsUndefinedFilter[Store <: StoreType, Value](field: StoreField[Store, Value])
    extends StoreFieldFilter[Store, Value] {

    /** Test a record */
    override def test(record: Store#Record): Boolean = field.get(record).nonEmpty
  }

  def isUndefined[Store <: StoreType, Value](field: StoreField[Store, Value]):
  StoreFieldFilter.IsUndefinedFilter[Store, Value]
  = IsUndefinedFilter(field)

}


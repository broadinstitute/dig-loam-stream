package loamstream.loam.ops.filters

import loamstream.loam.ops.{StoreField, StoreRecord, StoreType}

import scala.reflect.runtime.universe.{Type, TypeTag, typeTag}

/**
  * LoamStream
  * Created by oliverr on 11/17/2016.
  */
trait StoreFieldFilter[Store <: StoreType, Value]
  extends LoamStoreFilter[Store] {

  def tpe: Type

  def field: StoreField[Store, Value]

  /** Test a record, with dynamic type */
  override def testDynamicallyTyped(record: StoreRecord, tpe: Type): Boolean =
  (tpe <:< this.tpe) && test(record.asInstanceOf[Store#Record])

  /** Test a record */
  override def test(record: Store#Record): Boolean
}

object StoreFieldFilter {

  case class IsDefinedFilter[Store <: StoreType : TypeTag, Value](field: StoreField[Store, Value])
    extends StoreFieldFilter[Store, Value] {

    override val tpe: Type = typeTag[Store].tpe

    /** Test a record */
    override def test(record: Store#Record): Boolean = field.get(record).nonEmpty

  }

  def isDefined[Store <: StoreType : TypeTag, Value](field: StoreField[Store, Value]):
  StoreFieldFilter.IsDefinedFilter[Store, Value]
  = IsDefinedFilter(field)

  case class IsUndefinedFilter[Store <: StoreType : TypeTag, Value](field: StoreField[Store, Value])
    extends StoreFieldFilter[Store, Value] {

    override val tpe: Type = typeTag[Store].tpe

    /** Test a record */
    override def test(record: Store#Record): Boolean = field.get(record).isEmpty
  }

  def isUndefined[Store <: StoreType : TypeTag, Value](field: StoreField[Store, Value]):
  StoreFieldFilter.IsUndefinedFilter[Store, Value]
  = IsUndefinedFilter(field)

}


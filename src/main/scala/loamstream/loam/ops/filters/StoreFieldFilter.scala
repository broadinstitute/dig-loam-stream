package loamstream.loam.ops.filters

import loamstream.loam.ops.{StoreField, StoreRecord, StoreType}

import scala.reflect.runtime.universe.{Type, TypeTag, typeTag}

/**
  * LoamStream
  * Created by oliverr on 11/17/2016.
  */
trait StoreFieldFilter[S <: StoreType, V] extends LoamStoreFilter[S] {

  def tpe: Type

  /** Field of a store record used for filtering */
  def field: StoreField[S, V]

  /** Test a record, with dynamic type */
  override def testDynamicallyTyped(record: StoreRecord, tpe: Type): Boolean = {
    (tpe <:< this.tpe) && test(record.asInstanceOf[S#Record])
  }

  /** Test a record */
  override def test(record: S#Record): Boolean
}

object StoreFieldFilter {

  /** A filter to retain record where given field is defined */
  final case class IsDefinedFilter[S <: StoreType : TypeTag, V](field: StoreField[S, V])
      extends StoreFieldFilter[S, V] {

    override val tpe: Type = typeTag[S].tpe

    /** Test a record */
    override def test(record: S#Record): Boolean = field.get(record).nonEmpty

  }

  /** Return filter to retain record where given field is defined */
  def isDefined[S <: StoreType : TypeTag, V](field: StoreField[S, V]): IsDefinedFilter[S, V] = IsDefinedFilter(field)

  /** A filter to retain record where given field is undefined */
  final case class IsUndefinedFilter[S <: StoreType : TypeTag, V](field: StoreField[S, V])
      extends StoreFieldFilter[S, V] {

    override val tpe: Type = typeTag[S].tpe

    /** Test a record */
    override def test(record: S#Record): Boolean = field.get(record).isEmpty
  }

  /** Return filter to retain record where given field is defined */
  def isUndefined[S <: StoreType : TypeTag, V](field: StoreField[S, V]): IsUndefinedFilter[S, V] = {
    IsUndefinedFilter(field)
  }

}


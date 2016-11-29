package loamstream.loam.ops.filters

import loamstream.loam.ops.{LoamStoreOp, StoreRecord, StoreType}

import scala.reflect.runtime.universe.{Type, TypeTag}

/** A filter to be applied to records of a store */
object LoamStoreFilter {

  trait Untyped {
    /** Test a record, with dynamic type */
    def testDynamicallyTyped(record: StoreRecord, tpe: Type): Boolean
  }

}

/** A filter to be applied to records of a store */
abstract class LoamStoreFilter[S <: StoreType : TypeTag]
  extends LoamStoreOp[S, S] with LoamStoreFilter.Untyped {

  /** Test a record */
  def test(record: S#Record): Boolean

}

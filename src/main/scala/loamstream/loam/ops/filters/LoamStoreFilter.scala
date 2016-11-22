package loamstream.loam.ops.filters

import loamstream.loam.ops.{StoreRecord, StoreType}
import loamstream.loam.{LoamScriptContext, LoamStore}

import scala.reflect.runtime.universe.{Type, TypeTag}

/** A filter to be applied to records of a store */
object LoamStoreFilter {

  trait Untyped {
    /** Test a record, with dynamic type */
    def testDynamicallyTyped(record: StoreRecord, tpe: Type): Boolean
  }

}

/** A filter to be applied to records of a store */
abstract class LoamStoreFilter[S <: StoreType : TypeTag] extends LoamStoreFilter.Untyped {

  /** Test a record */
  def test(record: S#Record): Boolean

  /** A new suitable output store */
  def newOutStore(inStore: LoamStore[S])(implicit scriptContext: LoamScriptContext): LoamStore[S] = LoamStore.create[S]

  /** A new suitable Loam store filter tool */
  def newTool(inStore: LoamStore[S])(implicit scriptContext: LoamScriptContext): LoamStoreFilterTool[S] = {
    LoamStoreFilterTool(this, inStore, newOutStore(inStore))
  }

}

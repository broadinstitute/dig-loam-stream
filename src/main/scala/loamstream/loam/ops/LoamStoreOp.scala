package loamstream.loam.ops

import loamstream.loam.LoamScriptContext
import loamstream.model.Store

import scala.reflect.runtime.universe.TypeTag

/** An operation turning a store into another */
object LoamStoreOp {

  trait Untyped {
    /** A new suitable output store */
    def newOutStore(implicit scriptContext: LoamScriptContext): Store.Untyped
  }

}

/** An operation turning a store into another */
class LoamStoreOp[SI <: StoreType : TypeTag, SO <: StoreType : TypeTag] extends LoamStoreOp.Untyped {

  /** A new suitable output store */
  def newOutStore(implicit scriptContext: LoamScriptContext): Store[SO] = Store.create[SO]

}

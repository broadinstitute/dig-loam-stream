package loamstream.loam.ops.mappers

import loamstream.loam.ops.{LoamStoreOp, StoreRecord, StoreType}

import scala.reflect.runtime.universe.{Type, TypeTag}


/** A mapper to be applied to records of a store */
object LoamStoreMapper {

  trait Untyped {
    def mapDynamicallyTyped(record: StoreRecord, tpeIn: Type, tpeOut: Type): StoreRecord
  }

}

/** A mapper to be applied to records of a store */
abstract class LoamStoreMapper[SI <: StoreType : TypeTag, SO <: StoreType : TypeTag]
  extends LoamStoreOp[SI, SO] with LoamStoreMapper.Untyped {

  /** Map a record */
  def map(record: SI#Record): SO#Record

}

package loamstream.loam.ops.mappers

import loamstream.loam.LoamScriptContext
import loamstream.loam.ops.{LoamStoreOpTool, StoreType}
import loamstream.model.{LId, Store}

/** A tool based on a store mapper */
final case class LoamStoreMapperTool[SI <: StoreType, SO <: StoreType](id: LId,
                                                                       mapper: LoamStoreMapper[SI, SO],
                                                                       inStore: Store[SI],
                                                                       outStore: Store[SO])(
                                                                        implicit val scriptContext: LoamScriptContext)
  extends LoamStoreOpTool[SI, SO] {
  def op: LoamStoreMapper[SI, SO] = mapper
}

/** A tool based on a store mapper */
object LoamStoreMapperTool {
  def apply[SI <: StoreType, SO <: StoreType](mapper: LoamStoreMapper[SI, SO],
                                              inStore: Store[SI],
                                              outStore: Store[SO])(
                                               implicit scriptContext: LoamScriptContext):
  LoamStoreMapperTool[SI, SO] = {

    LoamStoreMapperTool(LId.newAnonId, mapper, inStore, outStore)
  }
}

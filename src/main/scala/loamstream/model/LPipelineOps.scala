package loamstream.model

import loamstream.model.kinds.LKind

/**
  * LoamStream
  * Created by oliverr on 2/25/2016.
  */
object LPipelineOps {

  def extractKeyStore(inputStore: StoreSpec, index: Int, kind: LKind): StoreSpec = {
    //TODO: NB: Fragile, what if no type at index?
    val keyType = inputStore.sig.keyTypes.asSeq(index)
    
    StoreSpec(LSig.Set.of(keyType), kind)
  }

  def extractKeyTool(inputStore: StoreSpec, index: Int, outputStore: StoreSpec): ToolSpec = {
    ToolSpec.keyExtraction(index)(inputStore, outputStore)
  }

  def extractKey(inputStore: StoreSpec, index: Int, outputStoreSpec: StoreSpec, kind: LKind): (StoreSpec, ToolSpec) = {
    val outputStoreSpec = extractKeyStore(inputStore, index, kind)
    
    val toolSpec = extractKeyTool(inputStore, index, outputStoreSpec)
    
    (outputStoreSpec, toolSpec)
  }
}

package loamstream.model

import loamstream.model.kinds.LKind
import loamstream.model.values.LType.LTuple.LTuple1
import loamstream.tools.core.CoreStore

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

  def extractKeyTool(inputStore: StoreSpec, index: Int, outputStore: StoreSpec): Tool = {
    Tool.keyExtraction(inputStore, outputStore, index)
  }

  def extractKey(inputStore: StoreSpec, index: Int, outputStoreSpec: StoreSpec, kind: LKind): (StoreSpec, Tool) = {
    val outputStoreSpec = extractKeyStore(inputStore, index, kind)
    
    val tool = extractKeyTool(inputStore, index, outputStoreSpec)
    
    (outputStoreSpec, tool)
  }

  def importVcfTool(inputStore: StoreSpec, index: Int, outputStore: StoreSpec): Tool = {
    Tool.vcfImport(inputStore, outputStore, index)
  }

  def calculateSingletonsTool(inputStore: StoreSpec, index: Int, outputStore: StoreSpec): Tool = {
    Tool.singletonCalculation(inputStore, outputStore, index)
  }
}

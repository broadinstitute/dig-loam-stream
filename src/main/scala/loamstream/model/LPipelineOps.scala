package loamstream.model

import loamstream.model.values.LType.LTuple.LTuple1
import loamstream.tools.core.CoreStore

/**
  * LoamStream
  * Created by oliverr on 2/25/2016.
  */
object LPipelineOps {

  def extractKeyStore(inputStore: Store, index: Int): Store = {
    //TODO: NB: Fragile, what if no type at index?
    val keyType = inputStore.spec.sig.keyTypes.asSeq(index)
    
    CoreStore("Extract Keys", LSig.Set.of(keyType))
  }

  def extractKeyTool(inputStore: Store, index: Int, outputStore: Store): Tool = {
    Tool.keyExtraction(inputStore, outputStore, index)
  }

  def extractKey(inputStore: Store, index: Int, outputStore: Store): (Store, Tool) = {
    val outputStore = extractKeyStore(inputStore, index)
    
    val tool = extractKeyTool(inputStore, index, outputStore)
    
    (outputStore, tool)
  }

  def importVcfTool(inputStore: Store, index: Int, outputStore: Store): Tool = {
    Tool.vcfImport(inputStore, outputStore, index)
  }

  def calculateSingletonsTool(inputStore: Store, index: Int, outputStore: Store): Tool = {
    Tool.singletonCalculation(inputStore, outputStore, index)
  }
}

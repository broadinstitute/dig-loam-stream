package loamstream.model

import loamstream.model.kinds.LKind
import loamstream.model.values.LType.LTuple.LTuple1
import loamstream.tools.core.CoreStore

/**
  * LoamStream
  * Created by oliverr on 2/25/2016.
  */
object LPipelineOps {

  def extractKeyStore(inputStore: Store, index: Int, kind: LKind): Store = {
    //TODO: NB: Fragile, what if no type at index?
    val keyType = inputStore.spec.sig.keyTypes.asSeq(index)
    
    CoreStore("Extract Keys", LSig.Set.of(keyType), kind)
  }

  def extractKeyTool(inputStore: Store, index: Int, outputStore: Store): Tool = {
    Tool.keyExtraction(inputStore, outputStore, index)
  }

  def extractKey(inputStore: Store, index: Int, outputStore: Store, kind: LKind): (Store, Tool) = {
    val outputPile = extractKeyStore(inputStore, index, kind)
    
    val recipe = extractKeyTool(inputStore, index, outputPile)
    
    (outputPile, recipe)
  }

  def importVcfTool(inputStore: Store, index: Int, outputStore: Store): Tool = {
    Tool.vcfImport(inputStore, outputStore, index)
  }

  def calculateSingletonsTool(inputStore: Store, index: Int, outputStore: Store): Tool = {
    Tool.singletonCalculation(inputStore, outputStore, index)
  }
}

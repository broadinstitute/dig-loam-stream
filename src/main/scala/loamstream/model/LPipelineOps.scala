package loamstream.model

import loamstream.model.kinds.LKind
import loamstream.model.piles.LSig
import loamstream.model.recipes.LRecipe
import loamstream.model.values.LType.LTuple.LTuple1
import loamstream.tools.core.CoreStore

/**
  * LoamStream
  * Created by oliverr on 2/25/2016.
  */
object LPipelineOps {

  def extractKeyPile(inputPile: StoreBase, index: Int, kind: LKind): StoreBase = {
    //TODO: NB: Fragile, what if no type at index?
    val keyType = inputPile.spec.sig.keyTypes.asSeq(index)
    
    CoreStore("Extract Keys", LSig.Set.of(keyType), kind)
  }

  def extractKeyRecipe(inputPile: StoreBase, index: Int, outputPile: StoreBase): LRecipe = {
    LRecipe.keyExtraction(inputPile, outputPile, index)
  }

  def extractKey(inputPile: StoreBase, index: Int, outputPile: StoreBase, kind: LKind): (StoreBase, LRecipe) = {
    val outputPile = extractKeyPile(inputPile, index, kind)
    
    val recipe = extractKeyRecipe(inputPile, index, outputPile)
    
    (outputPile, recipe)
  }

  def importVcfRecipe(inputPile: StoreBase, index: Int, outputPile: StoreBase): LRecipe = {
    LRecipe.vcfImport(inputPile, outputPile, index)
  }

  def calculateSingletonsRecipe(inputPile: StoreBase, index: Int, outputPile: StoreBase): LRecipe = {
    LRecipe.singletonCalculation(inputPile, outputPile, index)
  }
}

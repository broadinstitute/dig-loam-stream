package loamstream.model

import loamstream.model.kinds.LKind
import loamstream.model.piles.{LPile, LSig}
import loamstream.model.recipes.LRecipe
import loamstream.model.values.LType.LTuple

/**
  * LoamStream
  * Created by oliverr on 2/25/2016.
  */
object LPipelineOps {

  def extractKeyPile(inputPile: LPile, index: Int, kind: LKind): LPile =
    LPile(LSig.Set(LTuple(inputPile.spec.sig.keyTypes.types(index))), kind)

  def extractKeyRecipe(inputPile: LPile, index: Int, outputPile: LPile): LRecipe =
    LRecipe.keyExtraction(inputPile, outputPile, index)

  def extractKey(inputPile: LPile, index: Int, outputPile: LPile, kind: LKind): (LPile, LRecipe) = {
    val outputPile = extractKeyPile(inputPile, index, kind)
    val recipe = extractKeyRecipe(inputPile, index, outputPile)
    (outputPile, recipe)
  }

  def importVcfRecipe(inputPile: LPile, index: Int, outputPile: LPile): LRecipe =
    LRecipe.vcfImport(inputPile, outputPile, index)

  def calculateSingletonsRecipe(inputPile: LPile, index: Int, outputPile: LPile): LRecipe =
    LRecipe.singletonCalculation(inputPile, outputPile, index)

}

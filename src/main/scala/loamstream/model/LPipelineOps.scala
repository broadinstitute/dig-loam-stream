package loamstream.model

import loamstream.model.kinds.LKind
import loamstream.model.piles.{LPile, LPileSpec, LSig}
import loamstream.model.recipes.LRecipe

/**
  * LoamStream
  * Created by oliverr on 2/25/2016.
  */
object LPipelineOps {

  def extractKeyPile(inputPile: LPile, index: Int, kind: LKind) =
    LPile(LPileSpec(LSig.Set(Seq(inputPile.sig.keyTypes(index))), kind))

  def extractKeyRecipe(inputPile: LPile, index: Int, outputPile: LPile) =
    LRecipe.keyExtraction(inputPile, outputPile, index)

  def extractKey(inputPile: LPile, index: Int, outputPile: LPile, kind: LKind): (LPile, LRecipe) = {
    val outputPile = extractKeyPile(inputPile, index, kind)
    val recipe = extractKeyRecipe(inputPile, index, outputPile)
    (outputPile, recipe)
  }

}

package loamstream.model.recipes

import loamstream.model.kinds.LKind
import loamstream.model.piles.LPileSpec

import scala.language.higherKinds
import loamstream.model.kinds.ToolKinds

/**
  * LoamStream
  * Created by oliverr on 02/26/2016.
  */
object LRecipeSpec {

  def keyExtraction(index: Int)(input: LPileSpec, output: LPileSpec): LRecipeSpec = {
    LRecipeSpec(ToolKinds.extractKey(index), Seq(input), output)
  }

  def vcfImport(index: Int)(input: LPileSpec, output: LPileSpec): LRecipeSpec = {
    LRecipeSpec(ToolKinds.importVcf(index), Seq(input), output)
  }

  def calculateSingletons(index: Int)(input: LPileSpec, output: LPileSpec): LRecipeSpec = {
    LRecipeSpec(ToolKinds.calculateSingletons(index), Seq(input), output)
  }

  def preExistingCheckout(id: String)(output: LPileSpec): LRecipeSpec = {
    LRecipeSpec(ToolKinds.usePreExisting(id), Seq.empty[LPileSpec], output)
  }
}

case class LRecipeSpec(kind: LKind, inputs: Seq[LPileSpec], output: LPileSpec) {
  //TODO: non-symbolic alternative defs for each operator, a la LKind
  
  //TODO: Test if input arities are the same?
  //TODO Test if outputs are the same?
  def =:=(oRecipe: LRecipeSpec): Boolean = {
    kind == oRecipe.kind && 
    inputs.zip(oRecipe.inputs).forall { case (mine, theirs) => mine =:= theirs }
  }

  def <:<(oRecipe: LRecipeSpec): Boolean = doOperator(kindOp = _.<:<, inputOp = _.>:>, outputOp = _.<:<)(oRecipe)

  def >:>(oRecipe: LRecipeSpec): Boolean = doOperator(kindOp = _.>:>, inputOp = _.<:<, outputOp = _.>:>)(oRecipe)

  def <<<(oRecipe: LRecipeSpec): Boolean = doOperator(kindOp = _.<:<, inputOp = _.<:<, outputOp = _.<:<)(oRecipe)

  def >>>(oRecipe: LRecipeSpec): Boolean = doOperator(kindOp = _.>:>, inputOp = _.>:>, outputOp = _.>:>)(oRecipe)
    
  //NB: Stay DRY
  private def doOperator(
      kindOp: LKind => LKind => Boolean,
      inputOp: LPileSpec => LPileSpec => Boolean,
      outputOp: LPileSpec => LPileSpec => Boolean)(oRecipe: LRecipeSpec): Boolean = {
    
    kindOp(kind)(oRecipe.kind) &&
    inputs.size == oRecipe.inputs.size &&
    inputs.zip(oRecipe.inputs).forall { case (mine, theirs) => inputOp(mine)(theirs) } &&
    outputOp(output)(oRecipe.output)
  }
}

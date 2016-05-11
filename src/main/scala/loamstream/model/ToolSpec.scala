package loamstream.model

import scala.language.higherKinds

import loamstream.model.kinds.LKind
import loamstream.model.kinds.ToolKinds

/**
  * LoamStream
  * Created by oliverr on 02/26/2016.
  */
final case class ToolSpec(kind: LKind, inputs: Seq[StoreSpec], outputs: Seq[StoreSpec]) {
  //TODO: non-symbolic alternative defs for each operator, a la LKind
  
  //TODO: Test if input arities are the same?
  //TODO Test if outputs are the same?
  def =:=(other: ToolSpec): Boolean = {
    kind == other.kind && 
    inputs.zip(other.inputs).forall { case (mine, theirs) => mine =:= theirs }
  }

  def <:<(other: ToolSpec): Boolean = doOperator(kindOp = _.<:<, inputOp = _.>:>, outputOp = _.<:<)(other)

  def >:>(other: ToolSpec): Boolean = doOperator(kindOp = _.>:>, inputOp = _.<:<, outputOp = _.>:>)(other)

  def <<<(other: ToolSpec): Boolean = doOperator(kindOp = _.<:<, inputOp = _.<:<, outputOp = _.<:<)(other)

  def >>>(other: ToolSpec): Boolean = doOperator(kindOp = _.>:>, inputOp = _.>:>, outputOp = _.>:>)(other)
    
  //NB: Stay DRY
  private def doOperator(
      kindOp: LKind => LKind => Boolean,
      inputOp: StoreSpec => StoreSpec => Boolean,
      outputOp: StoreSpec => StoreSpec => Boolean)(other: ToolSpec): Boolean = {
    
    kindOp(kind)(other.kind) &&
    inputs.size == other.inputs.size &&
    inputs.zip(other.inputs).forall { case (mine, theirs) => inputOp(mine)(theirs) } &&
    outputs.zip(other.outputs).forall { case (mine, theirs) => outputOp(mine)(theirs) }
  }
}

object ToolSpec {

  def keyExtraction(index: Int)(input: StoreSpec, output: StoreSpec): ToolSpec = {
    ToolSpec(ToolKinds.extractKey(index), Seq(input), Seq(output))
  }

  def vcfImport(index: Int)(input: StoreSpec, output: StoreSpec): ToolSpec = {
    ToolSpec(ToolKinds.importVcf(index), Seq(input), Seq(output))
  }

  def calculateSingletons(index: Int)(input: StoreSpec, output: StoreSpec): ToolSpec = {
    ToolSpec(ToolKinds.calculateSingletons(index), Seq(input), Seq(output))
  }

  def preExistingCheckout(id: String)(output: StoreSpec): ToolSpec = {
    ToolSpec(ToolKinds.usePreExisting(id), Seq.empty[StoreSpec], Seq(output))
  }
}

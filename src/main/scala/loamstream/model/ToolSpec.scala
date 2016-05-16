package loamstream.model

import scala.language.higherKinds

import loamstream.model.kinds.LKind
import loamstream.model.kinds.ToolKinds

/**
  * LoamStream
  * Created by oliverr on 02/26/2016.
  */
final case class ToolSpec(kind: LKind, inputs: Map[LId, StoreSpec], outputs: Map[LId, StoreSpec]) {
  //TODO: non-symbolic alternative defs for each operator, a la LKind
  
  //TODO: Test if input arities are the same?
  //TODO Test if outputs are the same?
  def =:=(other: ToolSpec): Boolean = {
    kind == other.kind && 
    inputs.zip(other.inputs).forall { case ((myId, mine), (theirId, theirs)) => myId == theirId && mine =:= theirs }
  }

  private def andCompareIds(f: StoreSpec => StoreSpec => Boolean): ((LId, StoreSpec)) => ((LId, StoreSpec)) => Boolean = {
    case (lhsId, lhsSpec) => {
      case (rhsId, rhsSpec) => (lhsId == rhsId) && f(lhsSpec)(rhsSpec)
    }
  }
  
  def <:<(other: ToolSpec): Boolean = {
    doOperator(kindOp = _.<:<, inputOp = andCompareIds(_.>:>), outputOp = andCompareIds(_.<:<))(other)
  }

  def >:>(other: ToolSpec): Boolean = {
    doOperator(kindOp = _.>:>, inputOp = andCompareIds(_.<:<), outputOp = andCompareIds(_.>:>))(other)
  }

  def <<<(other: ToolSpec): Boolean = {
    doOperator(kindOp = _.<:<, inputOp = andCompareIds(_.<:<), outputOp = andCompareIds(_.<:<))(other)
  }

  def >>>(other: ToolSpec): Boolean = {
    doOperator(kindOp = _.>:>, inputOp = andCompareIds(_.>:>), outputOp = andCompareIds(_.>:>))(other)
  }
    
  //NB: Stay DRY
  private def doOperator(
      kindOp: LKind => LKind => Boolean,
      inputOp: ((LId, StoreSpec)) => ((LId, StoreSpec)) => Boolean,
      outputOp: ((LId, StoreSpec)) => ((LId, StoreSpec)) => Boolean)(other: ToolSpec): Boolean = {
    
    kindOp(kind)(other.kind) &&
    inputs.size == other.inputs.size &&
    inputs.zip(other.inputs).forall { case (mine, theirs) => inputOp(mine)(theirs) } &&
    outputs.zip(other.outputs).forall { case (mine, theirs) => outputOp(mine)(theirs) }
  }
}

object ToolSpec {

  object ParamNames {
    val input = LId.LNamedId("input")
    val output = LId.LNamedId("output")
  }
  
  def keyExtraction(index: Int)(input: StoreSpec, output: StoreSpec): ToolSpec = {
    ToolSpec(ToolKinds.extractKey(index), Map(ParamNames.input -> input), Map(ParamNames.output -> output))
  }

  def vcfImport(index: Int)(input: StoreSpec, output: StoreSpec): ToolSpec = {
    ToolSpec(ToolKinds.importVcf(index), Map(ParamNames.input -> input), Map(ParamNames.output -> output))
  }

  def calculateSingletons(index: Int)(input: StoreSpec, output: StoreSpec): ToolSpec = {
    ToolSpec(ToolKinds.calculateSingletons(index), Map(ParamNames.input -> input), Map(ParamNames.output -> output))
  }

  def preExistingCheckout(id: String)(output: StoreSpec): ToolSpec = {
    ToolSpec(ToolKinds.usePreExisting(id), Map.empty, Map(ParamNames.output -> output))
  }
}

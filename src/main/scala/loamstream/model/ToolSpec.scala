package loamstream.model

import scala.language.higherKinds

/**
  * LoamStream
  * Created by oliverr on 02/26/2016.
  */
final case class ToolSpec(inputs: Map[LId, StoreSpec], outputs: Map[LId, StoreSpec]) {
  //TODO: non-symbolic alternative defs for each operator, a la LKind
  
  //TODO: Test if input arities are the same?
  //TODO Test if outputs are the same?
  def =:=(other: ToolSpec): Boolean = {
    inputs.zip(other.inputs).forall { case ((myId, mine), (theirId, theirs)) => myId == theirId && mine =:= theirs }
  }

  private def andCompareIds(
      f: StoreSpec => StoreSpec => Boolean): ((LId, StoreSpec)) => ((LId, StoreSpec)) => Boolean = {
        
    case (lhsId, lhsSpec) => {
      case (rhsId, rhsSpec) => (lhsId == rhsId) && f(lhsSpec)(rhsSpec)
    }
  }
  
  def <:<(other: ToolSpec): Boolean = {
    doOperator(inputOp = andCompareIds(_.>:>), outputOp = andCompareIds(_.<:<))(other)
  }

  def >:>(other: ToolSpec): Boolean = {
    doOperator(inputOp = andCompareIds(_.<:<), outputOp = andCompareIds(_.>:>))(other)
  }

  def <<<(other: ToolSpec): Boolean = {
    doOperator(inputOp = andCompareIds(_.<:<), outputOp = andCompareIds(_.<:<))(other)
  }

  def >>>(other: ToolSpec): Boolean = {
    doOperator(inputOp = andCompareIds(_.>:>), outputOp = andCompareIds(_.>:>))(other)
  }
    
  //NB: Stay DRY
  private def doOperator(
      inputOp: ((LId, StoreSpec)) => ((LId, StoreSpec)) => Boolean,
      outputOp: ((LId, StoreSpec)) => ((LId, StoreSpec)) => Boolean)(other: ToolSpec): Boolean = {
    
    inputs.size == other.inputs.size &&
    inputs.zip(other.inputs).forall { case (mine, theirs) => inputOp(mine)(theirs) } &&
    outputs.zip(other.outputs).forall { case (mine, theirs) => outputOp(mine)(theirs) }
  }
}

object ToolSpec {
  
  object Indices {
    val variantKeyIndexInGenotypes: Int = 0
    val sampleKeyIndexInGenotypes: Int = 1
  }

  object ParamNames {
    val input = LId.LNamedId("input")
    val output = LId.LNamedId("output")
  }
  
  @deprecated("", "")
  def keyExtraction(index: Int)(input: StoreSpec, output: StoreSpec): ToolSpec = oneToOne(input, output)
  
  def oneToOne(input: StoreSpec, output: StoreSpec): ToolSpec = {
    ToolSpec(Map(ParamNames.input -> input), Map(ParamNames.output -> output))
  }

  @deprecated("", "")
  def vcfImport(index: Int)(input: StoreSpec, output: StoreSpec): ToolSpec = oneToOne(input, output)

  @deprecated("", "")
  def calculateSingletons(index: Int)(input: StoreSpec, output: StoreSpec): ToolSpec = oneToOne(input, output)

  @deprecated("", "")
  def preExistingCheckout(output: StoreSpec): ToolSpec = producing(output)
  
  def producing(output: StoreSpec): ToolSpec = {
    ToolSpec(Map.empty, Map(ParamNames.output -> output))
  }
}

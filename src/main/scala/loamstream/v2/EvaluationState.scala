package loamstream.v2

import EvaluationState.emptyMap
import loamstream.model.LId

object EvaluationState {
  private def emptyMap: Map[Tool, Set[LId]] = Map.empty.withDefaultValue(Set.empty)
  
  val Initial: EvaluationState = EvaluationState() 
}

final case class EvaluationState(
    symbols: SymbolTable = SymbolTable.Empty,
    inputs: Map[Tool, Set[LId]] = emptyMap,
    outputs: Map[Tool, Set[LId]] = emptyMap,
    toolStates: Map[Tool, ToolState] = Map.empty,
    firstPassComplete: Boolean = false,
    toolNames: Map[String, LId] = Map.empty) {
  
  def addInput(tool: Tool, input: LId): EvaluationState = {
    //NB: Won't throw, since inputs has a default value
    val inputsSoFar = inputs(tool) 
    val newInputs = inputsSoFar + input
    
    copy(inputs = inputs + (tool -> newInputs))
  }
  
  def addOutput(tool: Tool, input: LId): EvaluationState = {
    //NB: Won't throw, since outputs has a default value
    val outputsSoFar = outputs(tool) 
    val newOutputs = outputsSoFar + input
    
    copy(inputs = inputs + (tool -> newOutputs))
  }
  
  def nameTool(t: Tool, name: String): EvaluationState = copy(toolNames = toolNames + (name -> t.id))
  
  def addStore(s: Store): EvaluationState = copy(symbols = symbols + s)
  
  def addTool(t: Tool): EvaluationState = copy(symbols = symbols + t, toolStates = toolStates + (t -> ToolState.NotStarted))
  
  def updateToolState(newSnapshot: Tool.Snapshot): EvaluationState = {
    val oldToolState = toolStates.get(newSnapshot.tool).getOrElse(ToolState.NotStarted)
    
    if(newSnapshot.state == oldToolState) { 
      println(s"No transition to do: $this")
        
      this 
    } else {
      val newState = copy(toolStates = toolStates + newSnapshot.asTuple)
        
      println(s"Transitioned to: ${newSnapshot.state} from $oldToolState: ${newSnapshot.tool}")
      
      newState
    }
  }
  
  def finishFirstEvaluationPass(): EvaluationState = copy(firstPassComplete = true)
}

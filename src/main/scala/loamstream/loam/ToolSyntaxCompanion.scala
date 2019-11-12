package loamstream.loam

import loamstream.model.Tool

/**
 * @author clint
 * Oct 21, 2019
 */
trait ToolSyntaxCompanion[T <: Tool] {
  
  /** BEWARE: This method has the side-effect of modifying the graph within scriptContext */
  def addToGraph[U <: T](tool: U)(implicit scriptContext: LoamScriptContext): U = {
    scriptContext.projectContext.updateGraph { graph =>
      graph.withTool(tool, scriptContext)
    }
    
    tool
  }
}

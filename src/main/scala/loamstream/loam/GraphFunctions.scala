package loamstream.loam

import loamstream.model.Tool

/**
 * @author clint
 * Apr 9, 2020
 */
trait GraphFunctions {
  /** BEWARE: This method has the side-effect of modifying the graph within scriptContext */
  protected[loam] def addToGraph(tool: Tool)(implicit scriptContext: LoamScriptContext): Unit = {
    scriptContext.projectContext.updateGraph { graph =>
      graph.withTool(tool, scriptContext)
    }
  }
}

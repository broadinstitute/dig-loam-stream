package loamstream.loam.ast

import loamstream.loam.LoamGraph.StoreEdge
import loamstream.loam.{LoamGraph, LoamTool}
import loamstream.model.AST
import loamstream.model.AST.{Connection, ToolNode}
import loamstream.loam.LoamStore

/**
  * LoamStream
  * Created by oliverr on 6/16/2016.
  */
object LoamGraphAstMapper {

  val tempFilePrefix = "loam"
  
  def newMapping(graph: LoamGraph): LoamGraphAstMapping = {
    var toolsUnmapped: Set[LoamTool] = graph.tools
    var toolAsts: Map[LoamTool, AST] = Map.empty
    var rootTools: Set[LoamTool] = Set.empty
    var rootAsts: Set[AST] = Set.empty
    var makingProgress: Boolean = true
    
    def toConnection(inputStore: LoamStore): Option[Connection] = {
      graph.storeSources.get(inputStore) match {
        case Some(StoreEdge.ToolEdge(sourceTool)) =>
          val id = inputStore.id

          toolAsts.get(sourceTool).map(ast => Connection(id, id, ast))
        case _ => None
      }
    }
    
    def toAst(tool: LoamTool): AST = {
      val inputStores = graph.toolInputs.getOrElse(tool, Set.empty)
          
      val inputConnections = inputStores.flatMap(toConnection)
            
      ToolNode(tool, inputConnections)
    }
    
    def isRoot(tool: LoamTool): Boolean = graph.toolsSucceeding(tool).isEmpty
    
    def precedingToolsArePresent(tool: LoamTool): Boolean = {
      graph.toolsPreceding(tool).forall(toolAsts.contains)
    }
    
    while (toolsUnmapped.nonEmpty && makingProgress) {
      
      makingProgress = false
      
      for (tool <- toolsUnmapped) {
        if (precedingToolsArePresent(tool)) {
          val ast = toAst(tool)
          
          //TODO: This is fishy; after this line, the var we're iterating over with foreach() will be
          //a different object than the one pointed to by the updated 'toolsUnmapped' var. The tests 
          //pass, but I fear that may be by accident.
          toolsUnmapped -= tool
          
          toolAsts += tool -> ast
          
          if(isRoot(tool)) {
            rootTools += tool
            rootAsts += ast
          }
          
          makingProgress = true
        }
      }
    }
    
    LoamGraphAstMapping(graph, toolAsts, rootTools, rootAsts, toolsUnmapped)
  }
}

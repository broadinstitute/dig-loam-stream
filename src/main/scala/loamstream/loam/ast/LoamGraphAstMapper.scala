package loamstream.loam.ast

import loamstream.loam.LoamGraph.StoreEdge
import loamstream.loam.{LoamGraph, LoamTool}
import loamstream.model.AST
import loamstream.model.AST.{Connection, ToolNode}

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
    while (toolsUnmapped.nonEmpty && makingProgress) {
      makingProgress = false
      for (tool <- toolsUnmapped) {
        if (graph.toolsPreceding(tool).forall(toolAsts.contains)) {
          val inputStores = graph.toolInputs.getOrElse(tool, Set.empty)
          val inputConnections = inputStores.flatMap(inputStore =>
            graph.storeSources.get(inputStore) match {
              case Some(StoreEdge.ToolEdge(sourceTool)) =>
                val id = inputStore.id
                toolAsts.get(sourceTool).map(ast => Connection(id, id, ast))
              case _ => None
            })
          val ast = ToolNode(tool, inputConnections)
          toolsUnmapped -= tool
          toolAsts += tool -> ast
          if(graph.toolsSucceeding(tool).isEmpty) {
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

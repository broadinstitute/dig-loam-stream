package loamstream.loam.ast

import loamstream.loam.LoamGraph
import loamstream.loam.ast.AST.Connection
import loamstream.loam.ast.AST.ToolNode
import loamstream.model.Store
import loamstream.model.Tool

/**
  * LoamStream
  * Created by oliverr on 6/16/2016.
  */
object LoamGraphAstMapper {

  val tempFilePrefix = "loam"

  def newMapping(graph: LoamGraph): LoamGraphAstMapping = {
    var toolsUnmapped: Set[Tool] = graph.tools
    var toolAsts: Map[Tool, AST] = Map.empty
    var rootTools: Set[Tool] = Set.empty
    var rootAsts: Set[AST] = Set.empty
    var makingProgress: Boolean = true

    while (toolsUnmapped.nonEmpty && makingProgress) {

      makingProgress = false

      for (tool <- toolsUnmapped) {
        if (precedingToolsArePresent(graph, toolAsts)(tool)) {
          val ast = toAst(graph, toolAsts)(tool)

          //TODO: This is fishy; after this line, the var we're iterating over with foreach() will be
          //a different object than the one pointed to by the updated 'toolsUnmapped' var. The tests 
          //pass, but I fear that may be by accident.
          toolsUnmapped -= tool

          toolAsts += tool -> ast

          if (isRoot(graph)(tool)) {
            rootTools += tool
            rootAsts += ast
          }

          makingProgress = true
        }
      }
    }

    LoamGraphAstMapping(graph, toolAsts, rootTools, rootAsts, toolsUnmapped)
  }

  private def toConnection(
      graph: LoamGraph, 
      toolAsts: Map[Tool, AST])
      (inputStore: Store.Untyped): Option[Connection] = {

    graph.storeProducers.get(inputStore) match {
      case Some(sourceTool) =>
        val id = inputStore.id

        toolAsts.get(sourceTool).map(ast => Connection(id, id, ast))
      case _ => None
    }
  }

  private def toAst(graph: LoamGraph, toolAsts: Map[Tool, AST])(tool: Tool): AST = {
    val inputStores = graph.toolInputs.getOrElse(tool, Set.empty)

    val inputConnections = inputStores.flatMap(toConnection(graph, toolAsts))

    ToolNode(tool, inputConnections)
  }

  private def isRoot(graph: LoamGraph)(tool: Tool): Boolean = graph.toolsSucceeding(tool).isEmpty

  private def precedingToolsArePresent(graph: LoamGraph, toolAsts: Map[Tool, AST])(tool: Tool): Boolean = {
    graph.toolsPreceding(tool).forall(toolAsts.contains)
  }
}

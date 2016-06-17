package loamstream.loam.ast

import loamstream.loam.{LoamGraph, LoamTool}
import loamstream.model.AST

/**
  * LoamStream
  * Created by oliverr on 6/16/2016.
  */
object LoamGraphASTMapping {
  def apply(graph: LoamGraph): LoamGraphASTMapping = LoamGraphASTMapping(graph, Map.empty)
}

case class LoamGraphASTMapping(graph: LoamGraph, toolAsts: Map[LoamTool, AST]) {

  def withAst(tool: LoamTool, ast: AST): LoamGraphASTMapping = copy(toolAsts = toolAsts + (tool -> ast))

}

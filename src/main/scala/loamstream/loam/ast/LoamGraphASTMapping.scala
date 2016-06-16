package loamstream.loam.ast

import loamstream.loam.{LoamGraph, LoamStore, LoamTool}
import loamstream.model.{AST, Store, Tool}

/**
  * LoamStream
  * Created by oliverr on 6/16/2016.
  */
object LoamGraphASTMapping {
  def apply(graph: LoamGraph): LoamGraphASTMapping =
    LoamGraphASTMapping(graph, Map.empty, Map.empty, Map.empty, Map.empty)
}

case class LoamGraphASTMapping(graph: LoamGraph, stores: Map[LoamStore, Store], tools: Map[LoamTool, Tool],
                               loamToolAsts: Map[LoamTool, AST], toolAsts: Map[Tool, AST]) {

  def withStore(loamStore: LoamStore, store: Store): LoamGraphASTMapping =
    copy(stores = stores + (loamStore -> store))

  def withTool(loamTool: LoamTool, tool: Tool): LoamGraphASTMapping = copy(tools = tools + (loamTool -> tool))

  def withAst(loamTool: LoamTool, tool: Tool, ast: AST): LoamGraphASTMapping =
    copy(tools = tools + (loamTool -> tool), loamToolAsts = loamToolAsts + (loamTool -> ast),
      toolAsts = toolAsts + (tool -> ast))

}

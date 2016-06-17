package loamstream.loam.ast

import loamstream.loam.{LoamGraph, LoamTool}
import loamstream.model.AST

/**
  * LoamStream
  * Created by oliverr on 6/16/2016.
  */
case class LoamGraphAstMapping(graph: LoamGraph, toolAsts: Map[LoamTool, AST], rootTools: Set[LoamTool],
                               rootAsts: Set[AST], toolsUnmapped: Set[LoamTool]) {

  def complete: Boolean = toolsUnmapped.isEmpty
}

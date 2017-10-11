package loamstream.loam.ast

import loamstream.loam.LoamGraph
import loamstream.model.Tool

/**
  * LoamStream
  * Created by oliverr on 6/16/2016.
  */
final case class LoamGraphAstMapping(
    graph: LoamGraph,
    toolAsts: Map[Tool, AST],
    rootTools: Set[Tool],
    rootAsts: Set[AST],
    toolsUnmapped: Set[Tool]) {

  def complete: Boolean = toolsUnmapped.isEmpty
}

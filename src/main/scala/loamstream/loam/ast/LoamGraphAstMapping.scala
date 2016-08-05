package loamstream.loam.ast

import loamstream.loam.{LoamGraph, LoamCmdTool}
import loamstream.model.AST

/**
  * LoamStream
  * Created by oliverr on 6/16/2016.
  */
final case class LoamGraphAstMapping(
                                      graph: LoamGraph,
                                      toolAsts: Map[LoamCmdTool, AST],
                                      rootTools: Set[LoamCmdTool],
                                      rootAsts: Set[AST],
                                      toolsUnmapped: Set[LoamCmdTool]) {

  def complete: Boolean = toolsUnmapped.isEmpty
}

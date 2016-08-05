package loamstream.loam

import loamstream.model.{LId, Tool}
import loamstream.util.ValueBox

/** A tool defined in a Loam script */
trait LoamTool extends Tool {
  /** The unique tool id */
  def id: LId

  /** The ValueBox used to store the graph this tool is part of */
  def graphBox: ValueBox[LoamGraph]

  /** The graph this tool is part of */
  def graph: LoamGraph = graphBox.value

}

package loamstream.loam

import loamstream.loam.files.LoamFileManager
import loamstream.util.ValueBox

/** Container for compile time and run time context */
class LoamContext {

  val graphBox: ValueBox[LoamGraph] = new ValueBox(LoamGraph.empty)

  def graph: LoamGraph = graphBox.value

  val fileManager : LoamFileManager = new LoamFileManager

  val currentToolsBox: ValueBox[Map[Long, LoamNativeTool[_]]] = new ValueBox(Map.empty)

  def currentThreadId: Long = Thread.currentThread.getId

  def setCurrentTool(tool: LoamNativeTool[_]) : Unit = currentToolsBox(_ + (currentThreadId -> tool))

  def getCurrentTool: Option[LoamNativeTool[_]] = currentToolsBox.get(_.get(currentThreadId))

  def unsetCurrentTool(): Unit = currentToolsBox(_ - currentThreadId)

}

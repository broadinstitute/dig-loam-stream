package loamstream.loam.ast

import loamstream.loam.{LoamGraph, LoamTool}

/**
  * LoamStream
  * Created by oliverr on 6/16/2016.
  */
object LoamGraphASTMapper {

  val tempFilePrefix = "loam"

  case class TaskResult(mapping: LoamGraphASTMapping, moreTasks: Set[Task])

  sealed trait Task {
    def perform(loamGraphASTMapping: LoamGraphASTMapping): TaskResult
  }

  case class MapToolASTTask(tool: LoamTool) extends Task {
    override def perform(mapping: LoamGraphASTMapping): TaskResult = ??? // TODO
  }

  def newMapping(graph: LoamGraph): LoamGraphASTMapping = {
    var mapping = LoamGraphASTMapping(graph)
    var tasksCurrent: Set[Task] = graph.tools.map(MapToolASTTask)
    var tasksNext: Set[Task] = Set.empty
    while (tasksCurrent.nonEmpty || tasksNext.nonEmpty) {
      if (tasksCurrent.isEmpty) {
        tasksCurrent = tasksNext
        tasksNext = Set.empty
      }
      val task = tasksCurrent.head
      val taskResult = task.perform(mapping)
      mapping = taskResult.mapping
      tasksNext ++= (taskResult.moreTasks -- (tasksCurrent - task))
    }
    mapping
  }

}

package loamstream.loam.ast

import loamstream.loam.{LoamGraph, LoamStore, LoamTool}
import loamstream.model.Tool

/**
  * LoamStream
  * Created by oliverr on 6/16/2016.
  */
object LoamGraphASTMapper {

  case class TaskResult(mapping: LoamGraphASTMapping, moreTasks: Set[Task])

  sealed trait Task {
    def perform(loamGraphASTMapping: LoamGraphASTMapping): TaskResult
  }

  case class MapStoreTask(loamStore: LoamStore) extends Task {
    override def perform(loamGraphASTMapping: LoamGraphASTMapping): TaskResult = ??? // TODO
  }

  case class MapToolTask(loamTool: LoamTool) extends Task {
    override def perform(loamGraphASTMapping: LoamGraphASTMapping): TaskResult = ??? // TODO
  }

  case class MapToolASTTask(tool: Tool) extends Task {
    override def perform(loamGraphASTMapping: LoamGraphASTMapping): TaskResult = ??? // TODO
  }

  def newMapping(graph: LoamGraph): LoamGraphASTMapping = {
    var mapping = LoamGraphASTMapping(graph)
    var tasksCurrent: Set[Task] = graph.stores.map(MapStoreTask) ++ graph.tools.map(MapToolTask)
    var tasksNext: Set[Task] = Set.empty
    while (tasksCurrent.nonEmpty || tasksNext.nonEmpty) {
      if (tasksCurrent.isEmpty) {
        tasksCurrent = tasksNext
        tasksNext = Set.empty
      }
      val task = tasksCurrent.head
      val taskResult = task.perform(mapping)
      mapping = taskResult.mapping
      tasksNext ++= taskResult.moreTasks
    }
    mapping
  }

}

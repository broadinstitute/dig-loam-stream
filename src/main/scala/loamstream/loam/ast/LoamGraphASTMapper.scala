package loamstream.loam.ast

import java.nio.file.Files

import loamstream.loam.LoamGraph.StoreEdge
import loamstream.loam.files.FileSuffixes
import loamstream.loam.{LoamGraph, LoamStore, LoamTool}
import loamstream.model.{FileStore, StoreSpec, Tool}

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

  case class MapStoreTask(loamStore: LoamStore) extends Task {
    override def perform(mapping: LoamGraphASTMapping): TaskResult = {
      val storeSpec = StoreSpec(loamStore.tpe)
      val path = mapping.graph.storeSources.get(loamStore) match {
        case Some(StoreEdge.PathEdge(inputPath)) => inputPath
        case _ => mapping.graph.storeSinks.get(loamStore) match {
          case Some(StoreEdge.PathEdge(outputPath)) => outputPath
          case _ =>
            val suffix = FileSuffixes(loamStore.tpe)
            Files.createTempFile(tempFilePrefix, suffix)
        }
      }
      val store = FileStore(path, storeSpec)
      TaskResult(mapping.withStore(loamStore, store), Set.empty)
    }
  }

  case class MapToolTask(loamTool: LoamTool) extends Task {
    override def perform(mapping: LoamGraphASTMapping): TaskResult = ??? // TODO
  }

  case class MapToolASTTask(tool: Tool) extends Task {
    override def perform(mapping: LoamGraphASTMapping): TaskResult = ??? // TODO
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
      tasksNext ++= (taskResult.moreTasks -- (tasksCurrent - task))
    }
    mapping
  }

}

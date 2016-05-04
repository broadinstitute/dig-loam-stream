package loamstream.util

import loamstream.model.LPipeline
import loamstream.model.StoreSpec
import loamstream.model.Tool

/**
  * LoamStream
  * Created by oliverr on 3/24/2016.
  */
object PipelineConsistencyChecker {

  sealed trait Problem {
    def message: String
  }

  sealed trait Check extends (LPipeline => Set[Problem])

  case object NoStores extends Problem {
    override def message: String = "Pipeline contains no stores."
  }

  case object PipelineHasStoresCheck extends Check {
    override def apply(pipeline: LPipeline): Set[Problem] =
      if (pipeline.stores.isEmpty) Set(NoStores) else Set.empty
  }

  case object NoTools extends Problem {
    override def message: String = "Pipeline contains no tools"
  }

  case object PipelineHasToolsCheck extends Check {
    override def apply(pipeline: LPipeline): Set[Problem] =
      if (pipeline.tools.isEmpty) Set(NoTools) else Set.empty
  }

  sealed trait StoreSpecificProblem extends Problem {
    def store: StoreSpec
  }

  sealed trait ToolSpecificProblem extends Problem {
    def tool: Tool
  }

  sealed trait StoreIsNotProducedByExactlyOneTool extends StoreSpecificProblem

  case class StoreIsProducedByNoTool(store: StoreSpec) extends StoreIsNotProducedByExactlyOneTool {
    override def message: String = s"Store $store is not produced by any tool."
  }

  case object EachStoreIsOutputOfAtLeastOneToolCheck extends Check {
    override def apply(pipeline: LPipeline): Set[Problem] = {
      (pipeline.stores -- pipeline.tools.map(_.output)).map(StoreIsProducedByNoTool)
    }
  }

  case class StoreIsProducedByMultipleTools(store: StoreSpec, tools: Set[Tool])
    extends StoreIsNotProducedByExactlyOneTool {
    override def message: String =
      s"Store $store is produced by multiple tools: ${tools.map(_.id).mkString(", ")}."
  }

  case object EachStoreIsOutputOfNoMoreThanOneToolCheck extends Check {
    override def apply(pipeline: LPipeline): Set[Problem] = {
      pipeline.tools.groupBy(_.output).collect { case (store, tools) if tools.size > 1 =>
        val result: Problem = StoreIsProducedByMultipleTools(store, tools)
        
        result
      }.toSet[Problem]
    }
  }

  sealed trait StoreIsNotCompatibleWithTool extends StoreSpecificProblem with ToolSpecificProblem

  case class StoreIsIncompatibleOutputOfTool(store: StoreSpec, tool: Tool) extends StoreIsNotCompatibleWithTool {
    override def message: String = s"Store $store is not compatible output of tool ${tool.id}."
  }

  case object EachStoreIsCompatibleOutputOfToolCheck extends Check {
    override def apply(pipeline: LPipeline): Set[Problem] = {
      pipeline.tools.filterNot(tool => tool.output >:> tool.spec.output).
        map(tool => StoreIsIncompatibleOutputOfTool(tool.output, tool))
    }
  }

  case class StoreIsIncompatibleInputOfTool(store: StoreSpec, tool: Tool, pos: Int)
    extends StoreIsNotCompatibleWithTool {
    override def message: String =
      s"Store $store is not compatible input (position $pos) of tool ${tool.id}."
  }

  case object EachStoreIsCompatibleInputOfToolCheck extends Check {
    override def apply(pipeline: LPipeline): Set[Problem] = {
      pipeline.tools.flatMap { tool =>
        tool.inputs.indices.map(pos => (tool, pos, tool.inputs(pos), tool.spec.inputs(pos)))
      }.collect { case (tool, pos, input, inputSpec) if !(input <:< inputSpec) =>
        StoreIsIncompatibleInputOfTool(input, tool, pos)
      }
    }
  }

  sealed trait StoreMissingUsedInTool extends StoreSpecificProblem with ToolSpecificProblem

  case class StoreMissingUsedAsOutput(store: StoreSpec, tool: Tool) extends StoreMissingUsedInTool {
    override def message: String = s"Store $store used as output in tool ${tool.id} is missing."
  }

  case object EachOutputStoreIsPresentCheck extends Check {
    override def apply(pipeline: LPipeline): Set[Problem] = {
      pipeline.tools.collect { case tool if !pipeline.stores.contains(tool.output) =>
        StoreMissingUsedAsOutput(tool.output, tool)
      }
    }
  }

  case class StoreMissingUsedAsInput(store: StoreSpec, tool: Tool, pos: Int) extends StoreMissingUsedInTool {
    override def message: String = s"Store $store used as input (pos $pos) in tool ${tool.id} is missing."
  }

  case object EachInputStoreIsPresentCheck extends Check {
    override def apply(pipeline: LPipeline): Set[Problem] = {
      pipeline.tools.flatMap { tool => tool.inputs.indices.map(pos => (tool.inputs(pos), tool, pos)) }.
        collect { case (input, tool, pos) if !pipeline.stores.contains(input) =>
          StoreMissingUsedAsInput(input, tool, pos)
        }
    }
  }

  case class PipelineIsDisconnected(store: StoreSpec, otherStore: StoreSpec) extends StoreSpecificProblem {
    override def message: String = s"Pipeline is disconnected: no path from store $store to $otherStore."
  }

  case object ConnectednessCheck extends Check {
    override def apply(pipeline: LPipeline): Set[Problem] = {
      pipeline.stores.headOption match {
        case Some(arbitraryStore) => {
          var makingProgress = true
          var connectedStores: Set[StoreSpec] = Set(arbitraryStore)
          while (makingProgress) {
            makingProgress = false
            for (tool <- pipeline.tools) {
              val neighborStores = tool.inputs.toSet + tool.output
              val connectedStoresNew = neighborStores -- connectedStores
              if (connectedStoresNew.nonEmpty) {
                connectedStores ++= connectedStoresNew
                makingProgress = true
              }
            }
          }
          val otherStores = pipeline.stores -- connectedStores
          if (otherStores.nonEmpty) Set(PipelineIsDisconnected(arbitraryStore, otherStores.head)) else Set.empty
        }
        case None => Set.empty
      }
    }
  }

  case class PipelineHasCycle(store: StoreSpec) extends StoreSpecificProblem {
    override def message: String = s"Pipeline contains a cycle containing store $store."
  }

  case object AcyclicityCheck extends Check {
    override def apply(pipeline: LPipeline): Set[Problem] = {
      //TODO: Get rid of vars with a fold 
      var storesLeft = pipeline.stores
      var makingProgress = true
      var nStoresLeft = storesLeft.size
      while (storesLeft.nonEmpty && makingProgress) {
        storesLeft = pipeline.tools.filter(tool => storesLeft.contains(tool.output)).flatMap(_.inputs)
        val nStoresLeftNew = storesLeft.size
        makingProgress = nStoresLeftNew < nStoresLeft
        nStoresLeft = nStoresLeftNew
      }
      if (storesLeft.nonEmpty) Set(PipelineHasCycle(storesLeft.head)) else Set.empty
    }
  }

  val allChecks: Set[Check] = {
    Set(PipelineHasStoresCheck, PipelineHasToolsCheck, EachStoreIsOutputOfAtLeastOneToolCheck,
      EachStoreIsOutputOfNoMoreThanOneToolCheck, EachStoreIsCompatibleOutputOfToolCheck,
      EachStoreIsCompatibleInputOfToolCheck, EachOutputStoreIsPresentCheck, EachInputStoreIsPresentCheck,
      ConnectednessCheck, AcyclicityCheck)
  }

  def check(pipeline: LPipeline, checks: Set[Check] = allChecks): Set[Problem] = {
    checks.flatMap(check => check(pipeline))
  }
}

package loamstream.util

import loamstream.model.LPipeline
import loamstream.model.Store
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
    def store: Store
  }

  sealed trait ToolSpecificProblem extends Problem {
    def tool: Tool
  }

  sealed trait StoreIsNotProducedByExactlyOneTool extends StoreSpecificProblem

  case class StoreIsProducedByNoTool(store: Store) extends StoreIsNotProducedByExactlyOneTool {
    override def message: String = s"Store ${store.id} is not produced by any tool."
  }

  //TODO: TEST
  case object EachStoreIsOutputOfAtLeastOneToolCheck extends Check {
    override def apply(pipeline: LPipeline): Set[Problem] = {
      (pipeline.stores -- pipeline.tools.flatMap(_.outputs)).map(StoreIsProducedByNoTool)
    }
  }

  case class StoreIsProducedByMultipleTools(store: Store, tools: Set[Tool])
    extends StoreIsNotProducedByExactlyOneTool {
    override def message: String =
      s"Store ${store.id} is produced by multiple tools: ${tools.map(_.id).mkString(", ")}."
  }

  case object EachStoreIsOutputOfNoMoreThanOneToolCheck extends Check {
    override def apply(pipeline: LPipeline): Set[Problem] = {
      pipeline.toolsByOutput.collect { case (store, tools) if tools.size > 1 =>
        val result: Problem = StoreIsProducedByMultipleTools(store, tools)
        
        result
      }.toSet[Problem]
    }
  }

  sealed trait StoreIsNotCompatibleWithTool extends StoreSpecificProblem with ToolSpecificProblem

  case class StoreIsIncompatibleOutputOfTool(store: Store, tool: Tool) extends StoreIsNotCompatibleWithTool {
    override def message: String = s"Store ${store.id} is not compatible output of tool ${tool.id}."
  }

  case object EachStoreIsCompatibleOutputOfToolCheck extends Check {
    override def apply(pipeline: LPipeline): Set[Problem] = {
      //TODO: TEST
      pipeline.tools.filterNot { tool =>
        tool.outputs.map(_.spec).zip(tool.spec.outputs).forall {
          case (toolOutput, specOutput) => toolOutput >:> specOutput
        }
      }.flatMap { tool =>
        tool.outputs.map(output => StoreIsIncompatibleOutputOfTool(output, tool))
      }
    }
  }

  case class StoreIsIncompatibleInputOfTool(store: Store, tool: Tool, pos: Int)
    extends StoreIsNotCompatibleWithTool {
    override def message: String =
      s"Store ${store.id} is not compatible input (position $pos) of tool ${tool.id}."
  }

  case object EachStoreIsCompatibleInputOfToolCheck extends Check {
    override def apply(pipeline: LPipeline): Set[Problem] = {
      pipeline.tools.flatMap { tool =>
        tool.inputs.indices.map(pos => (tool, pos, tool.inputs(pos), tool.spec.inputs(pos)))
      }.collect { case (tool, pos, input, inputSpec) if !(input.spec <:< inputSpec) =>
        StoreIsIncompatibleInputOfTool(input, tool, pos)
      }
    }
  }

  sealed trait StoreMissingUsedInTool extends StoreSpecificProblem with ToolSpecificProblem

  case class StoreMissingUsedAsOutput(store: Store, tool: Tool) extends StoreMissingUsedInTool {
    override def message: String = s"Store ${store.id} used as output in tool ${tool.id} is missing."
  }

  case object EachOutputStoreIsPresentCheck extends Check {
    //TODO: TEST
    override def apply(pipeline: LPipeline): Set[Problem] = {
      def unspecifiedOutputsFor(tool: Tool): Seq[Store] = {
        tool.outputs.filterNot(pipeline.tools.flatMap(_.outputs).contains)
      }
      
      def someOutputIsNotSpecified(tool: Tool): Boolean = !unspecifiedOutputsFor(tool).isEmpty
      
      val toolsWithUnspecifiedOutputs = pipeline.tools.filter(someOutputIsNotSpecified)
      
      for {
        tool <- toolsWithUnspecifiedOutputs
        unspecifiedOutput <- unspecifiedOutputsFor(tool)
      } yield StoreMissingUsedAsOutput(unspecifiedOutput, tool)
    }
  }

  case class StoreMissingUsedAsInput(store: Store, tool: Tool, pos: Int) extends StoreMissingUsedInTool {
    override def message: String = s"Store ${store.id} used as input (pos $pos) in tool ${tool.id} is missing."
  }

  case object EachInputStoreIsPresentCheck extends Check {
    override def apply(pipeline: LPipeline): Set[Problem] = {
      pipeline.tools.flatMap { tool => tool.inputs.indices.map(pos => (tool.inputs(pos), tool, pos)) }.
        collect { case (input, tool, pos) if !pipeline.stores.contains(input) =>
          StoreMissingUsedAsInput(input, tool, pos)
        }
    }
  }

  case class PipelineIsDisconnected(store: Store, otherStore: Store) extends StoreSpecificProblem {
    override def message: String = s"Pipeline is disconnected: no path from store ${store.id} to ${otherStore.id}."
  }

  case object ConnectednessCheck extends Check {
    //TODO: TEST
    override def apply(pipeline: LPipeline): Set[Problem] = {
      pipeline.stores.headOption match {
        case Some(arbitraryStore) => {
          var makingProgress = true
          var connectedStores: Set[Store] = Set(arbitraryStore)
          while (makingProgress) {
            makingProgress = false
            for (tool <- pipeline.tools) {
              val neighborStores = tool.inputs.toSet ++ tool.outputs
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

  case class PipelineHasCycle(store: Store) extends StoreSpecificProblem {
    override def message: String = s"Pipeline contains a cycle containing store ${store.id}."
  }

  //TODO: TEST
  case object AcyclicityCheck extends Check {
    override def apply(pipeline: LPipeline): Set[Problem] = {
      //TODO: Get rid of vars with a fold 
      var storesLeft = pipeline.stores
      var makingProgress = true
      var nStoresLeft = storesLeft.size
      while (storesLeft.nonEmpty && makingProgress) {
        def keep(tool: Tool) = tool.outputs.exists(storesLeft.contains)
        
        storesLeft = pipeline.tools.filter(keep).flatMap(_.inputs)

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

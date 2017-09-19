package loamstream.cwl

import lenthall.validation.ErrorOr.{ErrorOr, MapErrorOrRhs, ShortCircuitingFlatMap}
import loamstream.loam.LoamGraph
import loamstream.model.{Store, Tool}
import wdl4s.wdl.types.WdlFileType
import wdl4s.wom.callable.{Callable, WorkflowDefinition}
import wdl4s.wom.expression.WomExpression
import wdl4s.wom.graph.{CallNode, Graph, GraphNode, GraphNodeInputExpression, GraphNodePort, RequiredGraphInputNode, TaskCallNode}

/**
  * LoamStream
  * Created by oliverr on 9/14/2017.
  */
object LoamToWom {

  def getNode(inputStore: Store.Untyped): RequiredGraphInputNode =
    RequiredGraphInputNode(inputStore.id.name, WdlFileType)

  def mapInputsToNodes(inputStores: Set[Store.Untyped]): Map[Store.Untyped, RequiredGraphInputNode] =
    inputStores.map { store =>
      val inputNode = getNode(store)
      (store, inputNode)
    }.toMap

  def getNode(tool: Tool): ErrorOr[TaskCallNode] = {
    val errorOrNodeAndInputs = CallNode.callWithInputs(
      name = ??? : String,
      callable = ??? : Callable,
      portInputs = ??? : Map[String, GraphNodePort.OutputPort],
      expressionInputs = ??? : Set[GraphNodeInputExpression]
    )
    errorOrNodeAndInputs.map(_.node.asInstanceOf[TaskCallNode])
  }

  def mapToolsToNodes(tools: Set[Tool]): ErrorOr[Map[Tool, TaskCallNode]] =
    tools.map { tool =>
      val toolNode = getNode(tool)
      (tool, toolNode)
    }.toMap.sequence


  def toWom(name: String, loam: LoamGraph): ErrorOr[WorkflowDefinition] = {
    val inputStoresToNodes = mapInputsToNodes(loam.inputStores)
    val inputNodes: Set[RequiredGraphInputNode] = inputStoresToNodes.values.toSet
    val errorOrToolsToNodes = mapToolsToNodes(loam.tools)
    errorOrToolsToNodes.flatMap { toolsToNodes =>
      val toolNodes: Set[TaskCallNode] = toolsToNodes.values.toSet
      val nodes: Set[GraphNode] = inputNodes ++ toolNodes
      val errorOrGraph = Graph.validateAndConstruct(nodes)
      errorOrGraph.map { graph =>
        val meta: Map[String, String] = ???
        val parameterMeta: Map[String, String] = ???
        val declarations: List[(String, WomExpression)] = ???
        WorkflowDefinition(name, graph, meta, parameterMeta, declarations)
      }
    }
  }

}

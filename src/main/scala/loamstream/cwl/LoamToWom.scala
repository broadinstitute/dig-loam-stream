package loamstream.cwl

import lenthall.validation.ErrorOr.ErrorOr
import loamstream.loam.LoamGraph
import loamstream.model.{Store, Tool}
import wdl4s.wdl.types.WdlFileType
import wdl4s.wom.callable.{TaskDefinition, WorkflowDefinition}
import wdl4s.wom.expression.WomExpression
import wdl4s.wom.graph.{Graph, GraphNode, RequiredGraphInputNode, TaskCallNode}

/**
  * LoamStream
  * Created by oliverr on 9/14/2017.
  */
object LoamToWom {

  def toWom(tool: Tool): ErrorOr[TaskDefinition] = {
    ???
  }

  def toWom(name: String, loam: LoamGraph): ErrorOr[WorkflowDefinition] = {
    val inputStoresToNodes: Map[Store.Untyped, RequiredGraphInputNode] = loam.inputStores.map { store =>
      val inputNode = RequiredGraphInputNode(store.id.name, WdlFileType)
      (store, inputNode)
    }.toMap
    val inputNodes: Set[RequiredGraphInputNode] = inputStoresToNodes.values.toSet
    val toolNodes: Set[TaskCallNode] = ???
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

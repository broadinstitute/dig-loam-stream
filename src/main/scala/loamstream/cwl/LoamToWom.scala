package loamstream.cwl

import cats.data.NonEmptyList
import cats.data.Validated.{Invalid, Valid}
import lenthall.validation.ErrorOr.{ErrorOr, MapErrorOrRhs, ShortCircuitingFlatMap}
import loamstream.loam.LoamToken.{MultiStoreToken, MultiToken, StoreRefToken, StoreToken, StringToken}
import loamstream.loam.{HasLocation, LoamCmdTool, LoamGraph}
import loamstream.model.{Store, Tool}
import wdl4s.wdl.WdlExpression
import wdl4s.wdl.command.{ParameterCommandPart, StringCommandPart}
import wdl4s.wdl.types.WdlFileType
import wdl4s.wom.callable.{Callable, TaskDefinition, WorkflowDefinition}
import wdl4s.wom.expression.WomExpression
import wdl4s.wom.graph.{CallNode, Graph, GraphNode, GraphNodeInputExpression, GraphNodePort, RequiredGraphInputNode, TaskCallNode}
import wdl4s.wom.{CommandPart, RuntimeAttributes}

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

  def storeToParameterCommandPart(hasLocation: HasLocation): ParameterCommandPart = {
    val expression: WdlExpression = ???
    val attributes: Map[String, String] = ???
    ParameterCommandPart(attributes, expression)
  }

  def addSpaces(parts: Seq[CommandPart]): Seq[CommandPart] =
    if (parts.size < 2) {
      parts
    } else {
      parts.head +: parts.tail.flatMap(part => Seq(StringCommandPart(" "), part))
    }

  def getCommandTemplate(tool: Tool): ErrorOr[Seq[CommandPart]] = {
    tool match {
      case cmdTool: LoamCmdTool =>
        val parts = cmdTool.tokens.flatMap {
          case StringToken(string) => Seq(StringCommandPart(string))
          case StoreToken(store) => Seq(storeToParameterCommandPart(store))
          case StoreRefToken(storeRef) => Seq(storeToParameterCommandPart(storeRef.store))
          case MultiStoreToken(stores) => addSpaces(stores.toSeq.map(storeToParameterCommandPart))
          case MultiToken(as) => addSpaces(as.toSeq.map(thing => StringCommandPart(thing.toString)))
        }
        Valid(parts)
      case _ =>
        val toolClassName = tool.getClass.getCanonicalName
        Invalid(NonEmptyList.of(s"Can only convert command line tools to WOM, but not $tool, which is $toolClassName."))
    }
  }

  def getTaskDefinition(tool: Tool): ErrorOr[TaskDefinition] = {
    getCommandTemplate(tool).map { commandTemplate =>
      val name = tool.id.name
      val runtimeAttributes: RuntimeAttributes = ???
      val meta: Map[String, String] = ???
      val parameterMeta: Map[String, String] = ???
      val outputs: Set[Callable.OutputDefinition] = ???
      val inputs: List[_ <: Callable.InputDefinition] = ???
      TaskDefinition(name, commandTemplate, runtimeAttributes, meta, parameterMeta, outputs, inputs)
    }
  }

  def getNode(tool: Tool): ErrorOr[TaskCallNode] = {
    val name = tool.id.name
    val taskDefinition = getTaskDefinition(tool)
    val portInputs: Map[String, GraphNodePort.OutputPort] = ???
    val expressionInputs: Set[GraphNodeInputExpression] = ???
    getTaskDefinition(tool).flatMap { taskDefinition =>
      CallNode.callWithInputs(name, taskDefinition, portInputs, expressionInputs)
    }.map(_.node.asInstanceOf[TaskCallNode])
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

package loamstream.cwl

import cats.data.Validated.{Invalid, Valid}
import cats.data.{NonEmptyList, Validated}
import lenthall.validation.ErrorOr.{ErrorOr, MapErrorOrRhs, ShortCircuitingFlatMap}
import loamstream.loam.LoamToken.{MultiStoreToken, MultiToken, StoreRefToken, StoreToken, StringToken}
import loamstream.loam.{HasLocation, LoamCmdTool, LoamGraph, LoamStoreRef}
import loamstream.model.{Store, Tool}
import wdl4s.parser.WdlParser.Terminal
import wdl4s.wdl.WdlExpression
import wdl4s.wdl.command.{ParameterCommandPart, StringCommandPart}
import wdl4s.wdl.types.{WdlFileType, WdlType}
import wdl4s.wdl.values.{WdlFile, WdlValue}
import wdl4s.wom.callable.{Callable, TaskDefinition, WorkflowDefinition}
import wdl4s.wom.expression.{IoFunctionSet, WomExpression}
import wdl4s.wom.graph.{CallNode, Graph, GraphNode, GraphNodeInputExpression, GraphNodePort, RequiredGraphInputNode, TaskCallNode}
import wdl4s.wom.{CommandPart, RuntimeAttributes}

/**
  * LoamStream
  * Created by oliverr on 9/14/2017.
  */
object LoamToWom {

  case class WomFileVariable(name: String) extends WomExpression {
    override def inputs: Set[String] = Set(name)

    override def evaluateValue(inputValues: Map[String, WdlValue], ioFunctionSet: IoFunctionSet): ErrorOr[WdlFile] =
      Validated.fromOption[NonEmptyList[String], WdlValue](inputValues.get(name),
        NonEmptyList.of(s"Missing input $name")).flatMap { (value: WdlValue) =>
        value match {
          case file: WdlFile => Valid(file)
          case _ => Invalid(NonEmptyList.of(s"Type of $name should be file, but is ${value}."))
        }
      }


    override def evaluateType(inputTypes: Map[String, WdlType]): ErrorOr[WdlType] = Valid(WdlFileType)

    override def evaluateFiles(inputTypes: Map[String, WdlValue], ioFunctionSet: IoFunctionSet,
                               coerceTo: WdlType): ErrorOr[Set[WdlFile]] =
      evaluateValue(inputTypes, ioFunctionSet).map(Set(_))
  }

  def getNode(inputStore: Store.Untyped): RequiredGraphInputNode =
    RequiredGraphInputNode(inputStore.id.name, WdlFileType)

  def mapInputsToNodes(inputStores: Set[Store.Untyped]): Map[Store.Untyped, RequiredGraphInputNode] =
    inputStores.map { store =>
      val inputNode = getNode(store)
      (store, inputNode)
    }.toMap

  private def hasLocationToStore(hasLocation: HasLocation): Store.Untyped = hasLocation match {
    case store: Store.Untyped => store
    case LoamStoreRef(store, _) => store
  }

  def getWdlExpression(store: Store.Untyped): WdlExpression = {
    val name = store.id.name
    val astId = name.##
    val ast = new Terminal(astId, name, name, name, 0, 0)
    WdlExpression(ast)
  }

  def storeToParameterCommandPart(hasLocation: HasLocation): ParameterCommandPart = {
    val store = hasLocationToStore(hasLocation)
    val name = store.id.name
    val expression: WdlExpression = getWdlExpression(store)
    val attributes: Map[String, String] = Map("default" -> name)
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
          case MultiToken(as) => Seq(StringCommandPart(as.mkString(" ")))
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
      val runtimeAttributes: RuntimeAttributes = RuntimeAttributes(Map.empty)
      val meta: Map[String, String] = Map.empty
      val parameterMeta: Map[String, String] = Map.empty
      val outputs: Set[Callable.OutputDefinition] = tool.outputs.values.map { store =>
        val name = store.id.name
        Callable.OutputDefinition(name, WdlFileType, WomFileVariable(name))
      }.toSet
      val inputs: List[_ <: Callable.InputDefinition] = tool.inputs.values.map{ store =>
        Callable.RequiredInputDefinition(store.id.name, WdlFileType)
      }.toList
      TaskDefinition(name, commandTemplate, runtimeAttributes, meta, parameterMeta, outputs, inputs)
    }
  }

  def getNode(tool: Tool, graph: LoamGraph): ErrorOr[TaskCallNode] = {
    val name = tool.id.name
    val (inputStores, producedStores) = tool.inputs.values.partition(graph.inputStores)
    val portInputs: Map[String, GraphNodePort.OutputPort] = producedStores.map { store =>
      val producer = graph.storeProducers(store)
      ???
    }.toMap
    val expressionInputs: Set[GraphNodeInputExpression] = ???
    getTaskDefinition(tool).flatMap { taskDefinition =>
      CallNode.callWithInputs(name, taskDefinition, portInputs, expressionInputs)
    }.map(_.node.asInstanceOf[TaskCallNode])
  }

  def mapToolsToNodes(graph: LoamGraph): ErrorOr[Map[Tool, TaskCallNode]] =
    graph.tools.map { tool =>
      val toolNode = getNode(tool, graph)
      (tool, toolNode)
    }.toMap.sequence


  def toWom(name: String, loam: LoamGraph): ErrorOr[WorkflowDefinition] = {
    val inputStoresToNodes = mapInputsToNodes(loam.inputStores)
    val inputNodes: Set[RequiredGraphInputNode] = inputStoresToNodes.values.toSet
    val errorOrToolsToNodes = mapToolsToNodes(loam)
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

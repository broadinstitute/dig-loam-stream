package loamstream.cwl

import cats.data.Validated.{Invalid, Valid}
import cats.data.{NonEmptyList, Validated}
import lenthall.validation.ErrorOr.{ErrorOr, MapErrorOrRhs, ShortCircuitingFlatMap}
import loamstream.loam.LoamToken.{MultiStoreToken, MultiToken, StoreRefToken, StoreToken, StringToken}
import loamstream.loam.{HasLocation, LoamCmdTool, LoamGraph, LoamStoreRef}
import loamstream.model.{Store, Tool}
import loamstream.util.lines.Lines
import loamstream.util.lines.Lines.iterablePrinter
import wdl4s.parser.WdlParser.Terminal
import wdl4s.wdl.WdlExpression
import wdl4s.wdl.command.{ParameterCommandPart, StringCommandPart}
import wdl4s.wdl.types.{WdlFileType, WdlType}
import wdl4s.wdl.values.{WdlFile, WdlValue}
import wdl4s.wom.callable.{Callable, TaskDefinition, WorkflowDefinition}
import wdl4s.wom.expression.{IoFunctionSet, WomExpression}
import wdl4s.wom.graph.{Graph, GraphNode, RequiredGraphInputNode, TaskCall}
import wdl4s.wom.{CommandPart, RuntimeAttributes}
import WomDebug.{graphNodePrinter, graphPrinter}

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
      val outputs: List[Callable.OutputDefinition] = tool.outputs.values.map { store =>
        val name = store.id.name
        Callable.OutputDefinition(name, WdlFileType, WomFileVariable(name))
      }.toList
      val inputs: List[_ <: Callable.InputDefinition] = tool.inputs.values.map { store =>
        Callable.RequiredInputDefinition(store.id.name, WdlFileType)
      }.toList
      TaskDefinition(name, commandTemplate, runtimeAttributes, meta, parameterMeta, outputs, inputs)
    }
  }

  def getToolGraph(tool: Tool, graph: LoamGraph): ErrorOr[Graph] = {
    val errorOrTaskDefinition = getTaskDefinition(tool)
    val errorOrToolGraph = errorOrTaskDefinition.flatMap(TaskCall.graphFromDefinition)
    errorOrToolGraph
  }

  def mapToolsToGraphs(graph: LoamGraph): ErrorOr[Map[Tool, Graph]] =
    graph.tools.map { tool =>
      val toolNode = getToolGraph(tool, graph)
      (tool, toolNode)
    }.toMap.sequence

  // scalastyle:off regex
  def toWom(name: String, loam: LoamGraph): ErrorOr[WorkflowDefinition] = {
    val inputStoresToNodes = mapInputsToNodes(loam.inputStores)
    val inputNodes: Set[RequiredGraphInputNode] = inputStoresToNodes.values.toSet
    println(s"Input nodes: ${Lines.toLines(inputNodes).asString}")
    val errorOrToolsGraphs = mapToolsToGraphs(loam)
    errorOrToolsGraphs.flatMap { toolsToGraphs =>
      val toolGraphs: Set[Graph] = toolsToGraphs.values.toSet
      println(s"Tool graphs: ${Lines.toLines(toolGraphs).asString}")
      val nodes: Set[GraphNode] = inputNodes ++ toolGraphs.map(_.nodes).fold(Set.empty)(_ ++ _)
      println(nodes.map(_.name).mkString("\n", "\n", "\n"))
      val errorOrGraph = Graph.validateAndConstruct(nodes)
      errorOrGraph.map { graph =>
        val meta: Map[String, String] = Map.empty
        val parameterMeta: Map[String, String] = Map.empty
        val declarations: List[(String, WomExpression)] = inputNodes.map { node =>
          (node.name, WomFileVariable(node.name))
        }.toList
        WorkflowDefinition(name, graph, meta, parameterMeta, declarations)
      }
    }
  }

}

package loamstream.cwl

import cats.data.Validated.{Invalid, Valid}
import cats.data.{NonEmptyList, Validated}
import lenthall.validation.ErrorOr.{ErrorOr, MapErrorOrRhs, ShortCircuitingFlatMap}
import loamstream.cwl.WomDebug.{graphNodePrinter, graphPrinter}
import loamstream.loam.LoamToken.{MultiStoreToken, MultiToken, StoreRefToken, StoreToken, StringToken}
import loamstream.loam.{HasLocation, LoamCmdTool, LoamGraph, LoamStoreRef}
import loamstream.model.{LId, Store, Tool}
import loamstream.util.lines.Lines
import loamstream.util.lines.Lines.iterablePrinter
import shapeless.Coproduct
import wdl4s.parser.WdlParser.Terminal
import wdl4s.wdl.WdlExpression
import wdl4s.wdl.command.{ParameterCommandPart, StringCommandPart}
import wdl4s.wdl.types.{WdlFileType, WdlType}
import wdl4s.wdl.values.{WdlFile, WdlValue}
import wdl4s.wom.callable.Callable.RequiredInputDefinition
import wdl4s.wom.callable.{Callable, TaskDefinition, WorkflowDefinition}
import wdl4s.wom.expression.{IoFunctionSet, WomExpression}
import wdl4s.wom.graph.CallNode.{InputDefinitionFold, InputDefinitionPointer}
import wdl4s.wom.graph.GraphNodePort.{GraphNodeOutputPort, InputPort}
import wdl4s.wom.graph.{CallNode, Graph, GraphInputNode, GraphNode, RequiredGraphInputNode, TaskCall, TaskCallNode}
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

  case class LoamToNodes(inputsToNodes: Map[LId, RequiredGraphInputNode], toolsToNodes: Map[LId, TaskCallNode]) {
    def plusToolsToNodes(toolsToNodesNew: Map[LId, TaskCallNode]): LoamToNodes =
      copy(toolsToNodes = toolsToNodes ++ toolsToNodesNew)
  }

  object LoamToNodes {
    val empty: LoamToNodes = LoamToNodes(Map.empty, Map.empty)

    def fromInputStores(inputs: Set[Store.Untyped]): LoamToNodes = {
      val inputsToNodes = inputs.map { input =>
        (input.id, RequiredGraphInputNode(input.id.name, WdlFileType))
      }.toMap
      LoamToNodes(inputsToNodes, Map.empty)
    }
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
    val ast = new Terminal(astId, "identifier", name, name, 0, 0)
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


  def toolToWomNodes(tool: Tool, loam: LoamGraph): ErrorOr[TaskCallNode] = {
    val errorOrTaskDefinition = getTaskDefinition(tool)
    errorOrTaskDefinition.map { taskDefinition =>
      val callNodeBuilder = new CallNode.CallNodeBuilder
      val name = tool.id.name
      val (producedInputs, providedInputs) =
        tool.inputs.partition({ case (_, toolInputStore) => loam.storeProducers.contains(toolInputStore) })
      val mappings: CallNode.InputDefinitionMappings = producedInputs.map { case (inputId, toolInputStore) =>
        val name = inputId.name
        val producerGraphNode: GraphNode = ???
        val inputDefinition = RequiredInputDefinition(name, WdlFileType)
        val outputPort = GraphNodeOutputPort(name, WdlFileType, producerGraphNode)
        val inputDefinitionPointer = Coproduct[InputDefinitionPointer](outputPort)
        (inputDefinition, inputDefinitionPointer)
      }
      val callInputPorts: Set[InputPort] = ???
      val newGraphInputNodes: Set[GraphInputNode] = ???
      val inputDefinitionFold = InputDefinitionFold(mappings, callInputPorts, newGraphInputNodes)
      callNodeBuilder.build(name, taskDefinition, inputDefinitionFold).node.asInstanceOf[TaskCallNode]
    }
  }

  def loamToWom(graphName: String, loam: LoamGraph): ErrorOr[WorkflowDefinition] = {
    val inputStoresToNodes = mapInputsToNodes(loam.inputStores)
    val inputNodes: Set[RequiredGraphInputNode] = inputStoresToNodes.values.toSet
    val nodes: Set[GraphNode] = ???
    val errorOrGraph = Graph.validateAndConstruct(nodes)
    errorOrGraph.map { graph =>
      val meta: Map[String, String] = Map.empty
      val parameterMeta: Map[String, String] = Map.empty
      val declarations: List[(String, WomExpression)] = inputNodes.map { node =>
        (node.name, WomFileVariable(node.name))
      }.toList
      WorkflowDefinition(graphName, graph, meta, parameterMeta, declarations)
    }
  }

  // scalastyle:off regex
  def toWomOldDoesntWork(name: String, loam: LoamGraph): ErrorOr[WorkflowDefinition] = {
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

package loamstream.cwl

import cats.data.Validated.{Invalid, Valid}
import cats.data.{NonEmptyList, Validated}
import cats.syntax.option.catsSyntaxOption
import lenthall.validation.ErrorOr.{ErrorOr, ShortCircuitingFlatMap}
import loamstream.loam.LoamToken.{MultiStoreToken, MultiToken, StoreRefToken, StoreToken, StringToken}
import loamstream.loam.{HasLocation, LoamCmdTool, LoamGraph, LoamStoreRef}
import loamstream.model.{LId, Store, Tool}
import shapeless.Coproduct
import wdl.WdlExpression
import wdl.command.{ParameterCommandPart, StringCommandPart}
import wdl.types.{WdlFileType, WdlType}
import wdl.values.{WdlFile, WdlValue}
import wdl4s.parser.WdlParser.Terminal
import wom.callable.Callable.{InputDefinition, RequiredInputDefinition}
import wom.callable.{Callable, TaskDefinition, WorkflowDefinition}
import wom.expression.{IoFunctionSet, WomExpression}
import wom.graph.CallNode.{InputDefinitionFold, InputDefinitionPointer}
import wom.graph.GraphNodePort.{InputPort, OutputPort}
import wom.graph.{CallNode, Graph, GraphInputNode, GraphNode, RequiredGraphInputNode, TaskCall, TaskCallNode}
import wom.{CommandPart, RuntimeAttributes}

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
          case _ => Invalid(NonEmptyList.of(s"Type of $name should be file, but is $value."))
        }
      }


    override def evaluateType(inputTypes: Map[String, WdlType]): ErrorOr[WdlType] = Valid(WdlFileType)

    override def evaluateFiles(inputTypes: Map[String, WdlValue], ioFunctionSet: IoFunctionSet,
                               coerceTo: WdlType): ErrorOr[Set[WdlFile]] =
      evaluateValue(inputTypes, ioFunctionSet).map(Set(_))
  }

  def getInputNodeName(id: LId): String = "store" + id.name

  def getTaskNodeName(id: LId): String = "task" + id.name

  def getPortName(id: LId): String = "store" + id.name

  case class LoamToNodes(loam: LoamGraph, inputsToNodes: Map[LId, RequiredGraphInputNode],
                         toolsToNodes: Map[LId, TaskCallNode]) {
    def plusToolsToNodes(toolsToNodesNew: Map[LId, TaskCallNode]): LoamToNodes =
      copy(toolsToNodes = toolsToNodes ++ toolsToNodesNew)

    def getNodeProvidingStore(store: Store.Untyped): ErrorOr[GraphNode] = {
      val inputNodeOption: Option[RequiredGraphInputNode] = inputsToNodes.get(store.id)
      inputNodeOption.orElse {
        loam.storeProducers.get(store).map(_.id).flatMap(toolsToNodes.get)
      }.toValidNel(s"No WOM node found for store $store.id.name}")
    }

    def getInputNodesFor(tool: Tool): Set[RequiredGraphInputNode] = {
      val toolInputStoresIds = tool.inputs.values.map(_.id).toSet
      inputsToNodes.filter {
        case (storeId, _) => toolInputStoresIds(storeId)
      }.values.toSet
    }

    def getAllNodes: Set[GraphNode] = (inputsToNodes.values ++ toolsToNodes.values).toSet
  }

  object LoamToNodes {

    def fromInputStores(loam: LoamGraph): LoamToNodes = {
      val inputsToNodes = loam.inputStores.map { input =>
        (input.id, RequiredGraphInputNode(getInputNodeName(input.id), WdlFileType))
      }.toMap
      LoamToNodes(loam, inputsToNodes, Map.empty)
    }
  }

  def getInputNode(inputStore: Store.Untyped): RequiredGraphInputNode =
    RequiredGraphInputNode(getInputNodeName(inputStore.id), WdlFileType)

  def mapInputsToNodes(inputStores: Set[Store.Untyped]): Map[Store.Untyped, RequiredGraphInputNode] =
    inputStores.map { store =>
      val inputNode = getInputNode(store)
      (store, inputNode)
    }.toMap

  private def hasLocationToStore(hasLocation: HasLocation): Store.Untyped = hasLocation match {
    case store: Store.Untyped => store
    case LoamStoreRef(store, _) => store
  }

  def getWdlExpression(store: Store.Untyped): WdlExpression = {
    val name = getPortName(store.id)
    val astId = name.##
    val ast = new Terminal(astId, "identifier", name, name, 0, 0)
    WdlExpression(ast)
  }

  def storeToParameterCommandPart(hasLocation: HasLocation): ParameterCommandPart = {
    val store = hasLocationToStore(hasLocation)
    val name = getPortName(store.id)
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
      val name = getTaskNodeName(tool.id)
      val runtimeAttributes: RuntimeAttributes = RuntimeAttributes(Map.empty)
      val meta: Map[String, String] = Map.empty
      val parameterMeta: Map[String, String] = Map.empty
      val outputs: List[Callable.OutputDefinition] = tool.outputs.values.map { store =>
        val name = getPortName(store.id)
        Callable.OutputDefinition(name, WdlFileType, WomFileVariable(name))
      }.toList
      val inputs: List[_ <: Callable.InputDefinition] = tool.inputs.values.map { store =>
        Callable.RequiredInputDefinition(getPortName(store.id), WdlFileType)
      }.toList
      TaskDefinition(name, commandTemplate, runtimeAttributes, meta, parameterMeta, outputs, inputs)
    }
  }

  def getToolGraph(tool: Tool, graph: LoamGraph): ErrorOr[Graph] = {
    val errorOrTaskDefinition = getTaskDefinition(tool)
    val errorOrToolGraph = errorOrTaskDefinition.flatMap(TaskCall.graphFromDefinition)
    errorOrToolGraph
  }

  object ErrorOrImplicits {

    implicit class MapOfErrorOr[K, V](mapOfErrorOr: Map[K, ErrorOr[V]]) {
      def sequence: ErrorOr[Map[K, V]] = if (mapOfErrorOr.values.forall(_.isValid)) {
        Valid(mapOfErrorOr.mapValues(_.asInstanceOf[Valid[V]].a).view.force)
      } else {
        val strings = mapOfErrorOr.values.collect { case Invalid(list) => list }.map(_.toList).fold(List.empty)(_ ++ _)
        println(strings.mkString("\n", "\n", "\n"))
        Invalid(NonEmptyList.fromListUnsafe(strings))
      }
    }

  }

  import ErrorOrImplicits.MapOfErrorOr

  def mapToolsToGraphs(graph: LoamGraph): ErrorOr[Map[Tool, Graph]] =
    graph.tools.map { tool =>
      val toolNode = getToolGraph(tool, graph)
      (tool, toolNode)
    }.toMap.sequence

  def toolToNode(tool: Tool, loam: LoamGraph, loamToNodes: LoamToNodes): ErrorOr[TaskCallNode] = {
    val errorOrTaskDefinition = getTaskDefinition(tool)
    errorOrTaskDefinition.flatMap { taskDefinition =>
      val callNodeBuilder = new CallNode.CallNodeBuilder
      val name = getTaskNodeName(tool.id)
      val errorOrInputDefinitionsToOutputPorts: ErrorOr[Map[InputDefinition, OutputPort]] =
        tool.inputs.map { case (_, toolInputStore) =>
          val inputName = getPortName(toolInputStore.id)
          val inputDefinition = RequiredInputDefinition(inputName, WdlFileType)
          val errorOrOutputPort = loamToNodes.getNodeProvidingStore(toolInputStore).map { providingNode =>
            providingNode.outputPorts.find(_.name == inputName).get // TODO Replace Option.get
          }
          (inputDefinition: InputDefinition, errorOrOutputPort)
        }.sequence
      val newGraphInputNodes = loamToNodes.getInputNodesFor(tool).map(_.asInstanceOf[GraphInputNode])
      errorOrInputDefinitionsToOutputPorts.map { inputDefinitionsToOutputPorts =>
        val mappings = inputDefinitionsToOutputPorts.mapValues(Coproduct[InputDefinitionPointer](_)).view.force
        val callInputPorts = inputDefinitionsToOutputPorts.map { case (inputDefinition, outputPort) =>
          callNodeBuilder.makeInputPort(inputDefinition, outputPort): InputPort
        }.toSet
        val inputDefinitionFold = InputDefinitionFold(mappings, callInputPorts, newGraphInputNodes)
        callNodeBuilder.build(name, taskDefinition, inputDefinitionFold).node.asInstanceOf[TaskCallNode]
      }
    }
  }

  def loamToWom(graphName: String, loam: LoamGraph): ErrorOr[WorkflowDefinition] = {
    loam.dagSortedTools.toErrorOr.flatMap { dagSortedTools =>
      val inputStoresToNodes = mapInputsToNodes(loam.inputStores)
      val inputNodes: Set[RequiredGraphInputNode] = inputStoresToNodes.values.toSet
      var errorOrLoamToNodes: ErrorOr[LoamToNodes] = Valid(LoamToNodes.fromInputStores(loam))
      for (toolsNext <- dagSortedTools) {
        errorOrLoamToNodes = errorOrLoamToNodes.flatMap { loamToNodes =>
          val errorOrToolsToNodesNew: ErrorOr[Map[LId, TaskCallNode]] = toolsNext.map { tool =>
            val errorOrTaskNode = toolToNode(tool, loam, loamToNodes)
            (tool.id, errorOrTaskNode)
          }.toMap.sequence
          errorOrToolsToNodesNew.map { toolsToNodesNew =>
            loamToNodes.plusToolsToNodes(toolsToNodesNew)
          }
        }
      }
      errorOrLoamToNodes.flatMap { loamToNodes =>
        val nodes: Set[GraphNode] = loamToNodes.getAllNodes
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
    }
  }

}

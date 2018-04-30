package loamstream.wdl

import loamstream.loam.LoamGraph
import loamstream.model.{Store, Tool}
import wdl.model.draft3.elements.{CommandSectionElement, ExpressionElement, FileBodyElement, FileElement, ImportElement, InputDeclarationElement, InputsSectionElement, IntermediateValueDeclarationElement, LanguageElement, MetaSectionElement, OutputsSectionElement, ParameterMetaSectionElement, PrimitiveTypeElement, RuntimeAttributesSectionElement, StructElement, TaskDefinitionElement, WorkflowDefinitionElement, WorkflowGraphElement}
import wom.types.WomStringType

object LoamToWdl {

  var workflowCounter: Int = 0
  def createWorkflowName: String = {
    val numberString = workflowCounter.toString
    val numberStringPadded = "0"*(3 - numberString.length) + numberString
    "workflow" + numberStringPadded
  }
  val defaultWorkflowName: String = "workflow"

  def getInputDeclarationElement(store: Store): InputDeclarationElement = {
    val typeElement = PrimitiveTypeElement(WomStringType)
    val name = store.id.name
    val expressionOpt: Option[ExpressionElement] = None
    InputDeclarationElement(typeElement, name, expressionOpt)
  }

  def getInputsMapping(loamGraph: LoamGraph): Map[Store, InputDeclarationElement] = {
    loamGraph.inputStores.map { store =>
      (store, getInputDeclarationElement(store))
    }.toMap
  }

  def getTask(tool: Tool, inputsMapping: Map[Store, InputDeclarationElement]): TaskDefinitionElement = {
    val name: String = tool.id.name
    val inputsSection: Option[InputsSectionElement] =
      Some(InputsSectionElement(tool.inputs.values.map(inputsMapping).toSeq))
    val declarations: Seq[IntermediateValueDeclarationElement] = Seq.empty
    val outputsSection: Option[OutputsSectionElement] = ???
    val commandSection: CommandSectionElement = ???
    val runtimeSection: Option[RuntimeAttributesSectionElement] = ???
    val metaSection: Option[MetaSectionElement] = ???
    val parameterMetaSection: Option[ParameterMetaSectionElement] = ???
    TaskDefinitionElement(
      name = name,
      inputsSection = inputsSection,
      declarations = declarations,
      outputsSection = outputsSection,
      commandSection = commandSection,
      runtimeSection = runtimeSection,
      metaSection = metaSection,
      parameterMetaSection = parameterMetaSection
    )

  }

  def getTasksMapping(loamGraph: LoamGraph, inputsMapping: Map[Store, InputDeclarationElement]):
  Map[Tool, TaskDefinitionElement] = {
    loamGraph.tools.map { tool =>
      (tool, getTask(tool, inputsMapping))
    }.toMap
  }

  def getWorkflow(loamGraph: LoamGraph): WorkflowDefinitionElement = {
    val name: String = createWorkflowName
    val inputsMapping = getInputsMapping(loamGraph)
    val inputsSection: Option[InputsSectionElement] = Some(InputsSectionElement(inputsMapping.values.toSeq))
    val graphElements: Set[WorkflowGraphElement] = ???
    val outputsSection: Option[OutputsSectionElement] = ???
    val metaSection: Option[MetaSectionElement] = ???
    val parameterMetaSection: Option[ParameterMetaSectionElement] = ???
    WorkflowDefinitionElement(
      name = name,
      inputsSection = inputsSection,
      graphElements = graphElements,
      outputsSection = outputsSection,
      metaSection = metaSection,
      parameterMetaSection = parameterMetaSection
    )
  }

  def loamToWdl(loamGraph: LoamGraph): FileElement = {
    val imports: Seq[ImportElement] = Seq.empty
    val structs: Seq[StructElement] = Seq.empty
    val inputsMapping = getInputsMapping(loamGraph)
    val tasksMapping = getTasksMapping(loamGraph, inputsMapping)
    val workflows: Seq[WorkflowDefinitionElement] = Seq(getWorkflow(loamGraph))
    val tasks: Seq[TaskDefinitionElement] = tasksMapping.values.toSeq
    FileElement(
      imports = imports,
      structs = structs,
      workflows = workflows,
      tasks = tasks
    )
  }

}

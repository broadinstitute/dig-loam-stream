package loamstream.wdl

import loamstream.loam.LoamGraph
import loamstream.model.{Store, Tool}
import wdl.model.draft3.elements.{ExpressionElement, FileElement, ImportElement, InputDeclarationElement, InputsSectionElement, LanguageElement, MetaSectionElement, OutputsSectionElement, ParameterMetaSectionElement, PrimitiveTypeElement, StructElement, TaskDefinitionElement, WorkflowDefinitionElement, WorkflowGraphElement}
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

  def getTaskMapping(loamGraph: LoamGraph): Map[Tool, TaskDefinitionElement] = {
    ???
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
    val workflows: Seq[WorkflowDefinitionElement] = Seq(getWorkflow(loamGraph))
    val tasks: Seq[TaskDefinitionElement] = ???
    FileElement(
      imports = imports,
      structs = structs,
      workflows = workflows,
      tasks = tasks
    )
  }

}

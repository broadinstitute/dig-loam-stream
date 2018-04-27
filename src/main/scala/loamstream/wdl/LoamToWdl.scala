package loamstream.wdl

import loamstream.loam.LoamGraph
import loamstream.model.Store
import wdl.model.draft3.elements.{ExpressionElement, InputDeclarationElement, InputsSectionElement, MetaSectionElement, OutputsSectionElement, ParameterMetaSectionElement, PrimitiveTypeElement, WorkflowDefinitionElement, WorkflowGraphElement}
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

  def loamToWdl(loamGraph: LoamGraph): WorkflowDefinitionElement = {
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

}

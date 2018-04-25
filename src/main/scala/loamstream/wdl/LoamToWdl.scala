package loamstream.wdl

import loamstream.loam.LoamGraph
import wdl.model.draft3.elements.{InputsSectionElement, MetaSectionElement, OutputsSectionElement}
import wdl.model.draft3.elements.{ParameterMetaSectionElement, WorkflowDefinitionElement, WorkflowGraphElement}

object LoamToWdl {

  var workflowCounter: Int = 0
  def createWorkflowName: String = {
    val numberString = workflowCounter.toString
    val numberStringPadded = "0"*(3 - numberString.length) + numberString
    "workflow" + numberStringPadded
  }
  val defaultWorkflowName: String = "workflow"

  def getInputSectionElement(loamGraph: LoamGraph): InputsSectionElement = {
    ???
  }

  def loamToWdl(loamGraph: LoamGraph): WorkflowDefinitionElement = {
    val name: String = createWorkflowName
    val inputsSection: Option[InputsSectionElement] = Some(getInputSectionElement(loamGraph))
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

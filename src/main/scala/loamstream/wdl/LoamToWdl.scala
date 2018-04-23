package loamstream.wdl

import loamstream.model.execute.Executable
import wdl.model.draft3.elements.{InputsSectionElement, MetaSectionElement, OutputsSectionElement}
import wdl.model.draft3.elements.{ParameterMetaSectionElement, WorkflowDefinitionElement, WorkflowGraphElement}

object LoamToWdl {

  def loamToWdl(executable: Executable): WorkflowDefinitionElement = {
    val name: String = ???
    val inputsSection: Option[InputsSectionElement] = ???
    val graphElements: Set[WorkflowGraphElement] = ???
    val outputsSection: Option[OutputsSectionElement] = ???
    val metaSection: Option[MetaSectionElement] = ???
    val parameterMetaSection: Option[ParameterMetaSectionElement] = ???
    WorkflowDefinitionElement(name, inputsSection, graphElements, outputsSection, metaSection, parameterMetaSection)
  }

}

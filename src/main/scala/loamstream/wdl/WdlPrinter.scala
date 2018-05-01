package loamstream.wdl

import loamstream.loam.LoamGraph
import wdl.model.draft3._

object WdlPrinter {

  /** Print single WDL element to a String. */
  def print(wdlElement: elements.LanguageElement): String = {
    wdlElement match {
      case it: elements.CallBodyElement           => printCallBody(it)
      case it: elements.CallElement               => printCall(it)
      case it: elements.CommandSectionElement     => printCommandSection(it)
      case it: elements.InputDeclarationElement   => printInput(it)
      case it: elements.InputsSectionElement      => printInputsSection(it)
      case it: elements.MetaSectionElement        => printMetaSection(it)
      case it: elements.OutputDeclarationElement  => printOutput(it)
      case it: elements.OutputsSectionElement     => printOutputsSection(it)
      case it: elements.TaskDefinitionElement     => printTaskDefinition(it)
      case it: elements.WorkflowDefinitionElement => printWorkflowDefinition(it)
      case it: elements.WorkflowGraphElement      => printWorkflowGraph(it)

      // not everything is handled
      case _ => throw new Exception("Unknown WDL LanguageElement!")
    }
  }

  /** Print a `call task [as [name]] { ... }` element. */
  private def printCall(it: elements.CallElement) = {
    s"""|call ${it.toString} ${it.alias.map(name => s"as $name")} {
        |  ${it.body.map(print _) getOrElse ""}
        |}
        |""".stripMargin
  }

  private def printCallBody(it: elements.CallBodyElement) = {
    val inputs = for (i <- it.inputs) yield {
      s"${i.key}=${i.value.toString}"
    }

    s"""|input:
        |  ${inputs mkString ",\n"}
        |""".stripMargin
  }

  /** Print a `command { ... }` element. */
  private def printCommandSection(it: elements.CommandSectionElement) = {
    s"""|command {
        |  ${it.toString}
        |}
        |""".stripMargin
  }

  /** Print a single input. */
  private def printInput(it: elements.InputDeclarationElement) = {
    s"${it.typeElement.toString} ${it.name}"
  }

  /** Print the input section. */
  private def printInputsSection(it: elements.InputsSectionElement) = {
    it.inputDeclarations.map(print _) mkString "\n"
  }

  /** Print a meta section. */
  private def printMetaSection(it: elements.MetaSectionElement) = {
    (for ((k, v) <- it.meta) yield s"$k = $v") mkString "\n"
  }

  /** Print a single output element. */
  private def printOutput(it: elements.OutputDeclarationElement) = {
    s"""${it.typeElement.toString} ${it.name} = "${it.toString}""""
  }

  /** Print the optional output section. */
  private def printOutputsSection(it: elements.OutputsSectionElement) = {
    s"""|output {
        |  ${it.outputs.map(print _) mkString "\n"}
        |}
        |""".stripMargin
  }

  /** Print a `task [name] { ... }` element. */
  private def printTaskDefinition(it: elements.TaskDefinitionElement) = {
    s"""|task ${it.name} {
        |  ${it.inputsSection.map(print _) getOrElse ""}
        |  ${print(it.commandSection)}
        |  ${it.outputsSection.map(print _) getOrElse ""}
        |}
        |""".stripMargin
  }

  /** Print a `workflow [name] { ... }` element. */
  private def printWorkflowDefinition(it: elements.WorkflowDefinitionElement) = {
    s"""|workflow ${it.name} {
        |  ${it.inputsSection.map(print _) getOrElse ""}
        |  ${it.outputsSection.map(print _) getOrElse ""}
        |  ${it.metaSection.map(print _) getOrElse ""}
        |  ${it.graphElements.map(print _) mkString "\n"}
        |}
        |""".stripMargin
  }

  private def printWorkflowGraph(it: elements.WorkflowGraphElement) = {
    ""
  }
}

package loamstream.wdl

import wdl.model.draft3._

object WdlPrinter {

  /** Print single WDL element to a String. */
  def print(wdlElement: elements.LanguageElement): String = {
    wdlElement match {
      case it: elements.CallBodyElement                 => printCallBody(it)
      case it: elements.CallElement                     => printCall(it)
      case it: elements.CommandSectionElement           => printCommandSection(it)
      case it: elements.InputDeclarationElement         => printInput(it)
      case it: elements.InputsSectionElement            => printInputsSection(it)
      case it: elements.MetaSectionElement              => printMetaSection(it)
      case it: elements.OutputDeclarationElement        => printOutput(it)
      case it: elements.OutputsSectionElement           => printOutputsSection(it)
      case it: elements.RuntimeAttributesSectionElement => printRuntimeSection(it)
      case it: elements.TaskDefinitionElement           => printTaskDefinition(it)
      case it: elements.WorkflowDefinitionElement       => printWorkflowDefinition(it)

      // not everything is handled
      case _ => throw new Exception("Unhandled WDL LanguageElement!")
    }
  }

  /** Print a single WDL expression element. */
  private def printExpression(expr: elements.ExpressionElement): String = {
    expr match {
      case it: elements.ExpressionElement.StringLiteral => s""""${it.value}""""

      // global output locations
      case elements.ExpressionElement.StderrElement     => "stderr()"
      case elements.ExpressionElement.StdoutElement     => "stdout()"

      // TODO: handle more expression element types
      case _ => throw new Exception("Unhandled WDL ExpressionElement")
    }
  }

  /** Print an element type. */
  private def printType(element: elements.TypeElement): String = {
    element match {
      case it: elements.ArrayTypeElement    => s"Array[${printType(it.inner)}]"
      case it: elements.MapTypeElement      => s"Map[${printType(it.keyType)},${printType(it.valueType)}]"
      case it: elements.OptionalTypeElement => s"Option[${printType(it.maybeType)}]"
      case it: elements.PairTypeElement     => s"Pair[${printType(it.leftType)},${printType(it.rightType)}]"

      // singleton types
      case elements.ObjectTypeElement => "Object"

      // primitive types
      case it: elements.PrimitiveTypeElement => {
        it.primitiveType match {
          case wom.types.WomBooleanType    => "Boolean"
          case wom.types.WomFloatType      => "Float"
          case wom.types.WomGlobFileType   => "Array[File]"
          case wom.types.WomIntegerType    => "Int"
          case wom.types.WomSingleFileType => "File"
          case wom.types.WomStringType     => "String"
        }
      }

      // TODO: handle more type elements
      case _ => throw new Error("Unhandled WDL TypeElement!")
    }
  }

  /** Print a `call task [as [name]] { ... }` element. */
  private def printCall(it: elements.CallElement) = {
    val alias = it.alias.map(name => s"as $name") getOrElse ""

    s"""|call ${it.callableReference} $alias {
        |  ${it.body.map(print _) getOrElse ""}
        |}
        |""".stripMargin
  }

  private def printCallBody(it: elements.CallBodyElement) = {
    val inputs = for (i <- it.inputs) yield {
      s"${i.key}=${printExpression(i.value)}"
    }

    s"""|input:
        |  ${inputs mkString ",\n"}
        |""".stripMargin
  }

  private def printCommandLinePart(part: elements.CommandPartElement) = {
    part match {
      case it: elements.CommandPartElement.StringCommandPartElement => it.value
      case it: elements.CommandPartElement.PlaceholderCommandPartElement => "TODO: "
    }
  }

  /** Print a command line part. */
  private def printCommandLine(it: elements.CommandSectionLine) = {
    s"${it.parts map (printCommandLinePart _) mkString " "}"
  }

  /** Print a `command { ... }` element. */
  private def printCommandSection(it: elements.CommandSectionElement) = {
    s"""|command {
        |  ${it.parts map (printCommandLine _) mkString "\n  "}
        |}
        |""".stripMargin
  }

  /** Print a single input. */
  private def printInput(it: elements.InputDeclarationElement) = {
    s"${printType(it.typeElement)} ${it.name}"
  }

  /** Print the input section. */
  private def printInputsSection(it: elements.InputsSectionElement) = {
    it.inputDeclarations.map(print _) mkString "\n  "
  }

  /** Print a meta section. */
  private def printMetaSection(it: elements.MetaSectionElement) = {
    (for ((k, v) <- it.meta) yield s"$k = $v") mkString "\n  "
  }

  /** Print a single output element. */
  private def printOutput(it: elements.OutputDeclarationElement) = {
    s"${printType(it.typeElement)} ${it.name} = ${printExpression(it.expression)}"
  }

  /** Print the optional output section. */
  private def printOutputsSection(it: elements.OutputsSectionElement) = {
    s"""|output {
        |  ${it.outputs.map(print _) mkString "\n  "}
        |}
        |""".stripMargin
  }

  /** Print a single KV pair element. */
  private def printKvPair(it: elements.ExpressionElement.KvPair) = {
    s"${it.key} = ${it.value}"
  }

  /** Print a `runtime { ... } attribute section. */
  private def printRuntimeSection(it: elements.RuntimeAttributesSectionElement) = {
    s"""|runtime {
        |  ${it.runtimeAttributes.map(printKvPair _) mkString ","}
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
}

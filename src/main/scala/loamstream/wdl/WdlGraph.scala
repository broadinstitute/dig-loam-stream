package loamstream.wdl

import java.io.OutputStream
import loamstream.loam.{LoamCmdTool, LoamGraph}
import loamstream.model.{Store, Tool}
import loamstream.util.Gensym
import wdl.model.draft3.elements
import wom.types._

/** Create a WDL task element from a LoamGraph node. */
class WdlGraph(val graph: LoamGraph) {

  /** Create an InputDeclarationElement from a Store. */
  private def inputOfStore(store: Store) = {
    val womType = elements.PrimitiveTypeElement(WomSingleFileType)
    val name = store.render.replaceAll("[^a-zA-Z0-9_]", "_")

    // create a new input declaration element that's a file
    elements.InputDeclarationElement(womType, name, None)
  }

  /** Create an OutputDeclarationElement from a Store. */
  private def outputOfStore(store: Store) = {
    val womType = elements.PrimitiveTypeElement(WomSingleFileType)
    val name = store.render.replaceAll("[^a-zA-Z0-9_]", "_")
    val expression = elements.ExpressionElement.StdoutElement

    // create a new output declaration element that's a file
    elements.OutputDeclarationElement(womType, name, expression)
  }

  /** Create a CommandSectionLine from a Tool. */
  private def commandLineOfTool(tool: Tool) = {
    tool match {
      case it: LoamCmdTool => {
        val part = elements.CommandPartElement.StringCommandPartElement(it.commandLine)

        // each command line is already interpolated by Loam
        elements.CommandSectionLine(Seq(part))
      }

      // TOOD: handle more tool types
      case _ => throw new Exception("Unhandled Tool type for WDL command section.")
    }
  }

  /**
   * Create the top-level WDL tasks.
   *
   * Every `Tool` in the graph corresponds to a script that will be executed.
   * Each of the tools has a set of inputs and outputs.
   */
  lazy val tasks = graph.tools map { tool =>
    val inputs = graph.toolInputs(tool) map (inputOfStore _)
    val outputs = graph.toolOutputs(tool) map (outputOfStore _)
    val declarations = Seq() // TODO: constant inputs?
    val runtimeSection = None // TODO: docker image
    val metaSection = None
    val parameterMetaSection = None

    // map inputs to an input section if there are any
    val inputsSection = Option(inputs.size > 0) collect {
      case true => elements.InputsSectionElement(inputs.toSeq)
    }

    // map outputs to an output section if there are any
    val outputsSection = Option(outputs.size > 0) collect {
      case true => elements.OutputsSectionElement(outputs.toSeq)
    }

    // take the script context and map that to a command section
    val commandSection = elements.CommandSectionElement(Seq(commandLineOfTool(tool)))

    // create the task
    elements.TaskDefinitionElement(
      Gensym("task"),
      inputsSection,
      declarations,
      outputsSection,
      commandSection,
      runtimeSection,
      metaSection,
      parameterMetaSection,
    )
  }

  /**
   * Create the top-level WDL workflow that manages calling all the tasks.
   */
  lazy val workflow = {
    // TODO:
  }

  /** Write this graph task node to a stream. */
  def write(stream: OutputStream) = {
    tasks foreach (task => stream write WdlPrinter.print(task).getBytes)
  }
}

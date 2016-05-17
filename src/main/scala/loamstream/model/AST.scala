package loamstream.model

import scala.util.Try
import loamstream.util.Tries
import loamstream.util.Maps

/**
 * @author clint
 * date: May 12, 2016
 *
 * Class representing the tree of relationships between tools in a pipeline.  Allows
 * composing trees and tools.
 */
sealed trait AST {
  def id: LId = LId.newAnonId

  import AST._

  def inputs: Set[NamedInput]

  def withInputs(inputs: Set[NamedInput]): AST

  final def <~(input: NamedInput): AST = dependsOn(input)

  final def dependsOn(input: NamedInput): AST = dependsOn(input.id, input.producer)
  final def dependsOn(inputId: LId, input: AST): AST = withInputs(inputs + NamedInput(inputId, input))

  final def dependsOn(inputId: LId): NamedDependency = NamedDependency(this, inputId)

  final def dependsOn(inputId: LId, rest: LId*): MultiNamedDependency = MultiNamedDependency(this, rest.toSet + inputId)

  final def output(outputId: LId): NamedInput = NamedInput(outputId, this)

  final def apply(outputId: LId): NamedInput = output(outputId)

  final def isLeaf: Boolean = inputs.isEmpty

  /**
   * Performs post-order traversal and invokes f for each node
   */
  def traverse(f: AST => Any): Unit = {
    inputs.foreach(_.producer.traverse(f))

    f(this)
  }

  def fold[R](z: R)(op: (R, AST) => R): R = {
    var acc = z

    traverse { node =>
      acc = op(acc, node)
    }

    acc
  }

  def print(indent: Int = 0, via: Option[LId] = None, doPrint: (String) => Any = println(_)): Unit = {
    val indentString = s"${"-" * indent}${via.map(v => s"($v)").getOrElse("")}> "

    doPrint(s"$indentString$id")

    inputs.foreach { case NamedInput(inputId, dep) => dep.print(indent + 2, Option(inputId), doPrint) }
  }

  def leaves: Set[AST] = {
    if (isLeaf) { Set(this) }
    else { inputs.flatMap(_.producer.leaves) }
  }
}

object AST {
  def apply(tool: Tool): AST = ToolNode(tool.id, tool.spec, Set.empty)

  object Implicits {
    final implicit class SeqOfASTsOps(val asts: Seq[AST]) extends AnyVal {
      def outputs(ids: LId.CompositeId*): Seq[NamedInput] = {
        val byId: Map[LId, AST] = asts.map(c => c.id -> c).toMap

        //NB: Can fail
        ids.collect { case LId.CompositeId(namespace, name) if byId.contains(namespace) => byId(namespace).output(name) }
      }
    }

    final implicit class SpecOps(val spec: ToolSpec) extends AnyVal {
      def as(id: LId): ToolNode = ToolNode(id, spec)
    }
  }

  final case class NamedDependency(consumer: AST, outputId: LId) {
    def from(producer: AST): AST = consumer.dependsOn(outputId, producer)
  }

  final case class MultiNamedDependency(consumer: AST, outputIds: Set[LId]) {
    def from(producer: AST): AST = {
      outputIds.foldLeft(consumer) { (ast, id) =>
        ast.dependsOn(id).from(producer)
      }
    }
  }

  final case class NamedInput(id: LId, producer: AST)

  final case class ToolNode(override val id: LId, spec: ToolSpec, inputs: Set[NamedInput] = Set.empty) extends AST {
    override def withInputs(newInputs: Set[NamedInput]): AST = copy(inputs = newInputs)
  }

  final case class Either(lhs: AST, rhs: AST, inputs: Set[NamedInput] = Set.empty) extends AST {
    override def withInputs(newInputs: Set[NamedInput]): AST = copy(inputs = newInputs)
  }

  final case class Parallel(components: Seq[AST], inputs: Set[NamedInput] = Set.empty) extends AST {
    override val id = LId.LNamedId(components.map(_.id).mkString(","))

    private lazy val byId: Map[LId, AST] = components.map(c => c.id -> c).toMap

    override def withInputs(newInputs: Set[NamedInput]): AST = copy(inputs = newInputs)

    def outputs(ids: LId.CompositeId*): Seq[NamedInput] = {
      ids.map { case LId.CompositeId(namespace, name) => byId(namespace).output(name) }
    }
  }

  /**
   * Given an LPipeline, return a Try[AST], representing an attempt at the tree of dependencies between
   * Tools in the pipeline.
   *
   * Returns Failure if pipeline contains no tools, or if exactly one terminal tool cannot be found,
   * returns Success otherwise.
   *
   */
  def fromPipeline(pipeline: LPipeline): Try[AST] = {
    if (pipeline.tools.isEmpty) {
      Tries.failure("No tools")
    } else {
      for {
        terminal <- findTerminalTool(pipeline)
      } yield {
        import Maps.Implicits._

        val byOutput = pipeline.toolsByOutput.mapKeys(_.spec)

        astFor(byOutput)(terminal)
      }
    }
  }

  /**
   * Given an LPipeline, finds the "last" Tool, the one who's output isn't the input
   * of any other tools.  This Tool can be seen as producing the "output" of a
   * pipeline.
   */
  private[model] def findTerminalTool(pipeline: LPipeline): Try[Tool] = {
    val tools = pipeline.tools

    val toolsAndOthers = tools.map(t => t -> (tools - t))

    final implicit class ToolOps(val t: Tool) {
      def takesNoInputFrom(otherTool: Tool): Boolean = {
        t.inputs.forall { case (inputId, inputSpec) =>
          !otherTool.outputs.exists { case (outputId, outputSpec) =>
            inputId == outputId && inputSpec == outputSpec
          }
        }
      }
    }

    def isNoOnesInput(tool: Tool, others: Set[Tool]): Boolean = {
      def inputsOf(t: Tool): Map[LId, StoreSpec] = t.spec.inputs
      def outputsOf(t: Tool): Map[LId, StoreSpec] = t.spec.outputs

      //No other tool takes any of `tool`'s outputs as input 
      others.forall(otherTool => otherTool.takesNoInputFrom(tool))
    }

    val terminals = toolsAndOthers.collect { case (tool, others) if isNoOnesInput(tool, others) => tool }

    if (terminals.size != 1) { Tries.failure(s"Expected 1 terminal tool, but found ${terminals.size}: $terminals") }
    else { Try(terminals.head) }
  }

  /**
   * Given a mapping tool outputs to sets of tools with that output spec, and a tool output spec,
   * Produce a tree of AST.Nodes prepresenting that tool, by walking out from that tool, making ASTs
   * for its inputs recursively.
   *
   * Runs in O(n) time, where n is the total number of tools the tool indicated by toolOutput
   * directly and transitively depends on.
   */
  private[model] def astFor(byOutput: Map[StoreSpec, Set[Tool]])(tool: Tool): AST = {
    /*def idOfInputSpec(inputSpec: StoreSpec): Option[LId] = {
      tool.spec.outputs.find { 
        case (_, outputSpec) => inputSpec == outputSpec 
      }.collect {
        case (id, _) => id
      }
    }*/
    
    val toolsProducingInputs: Set[(LId, Tool)] = for {
      (inputId, input) <- tool.inputs.toSet[(LId, Store)]
      inputSpec = input.spec
      producer <- byOutput.get(inputSpec).toSet.flatten
      //inputId <- idOfInputSpec(inputSpec).toSet[LId]
    } yield (inputId, producer)

    val inputTrees: Set[NamedInput] = toolsProducingInputs.map {
      case (inputId, toolProducingInput) => NamedInput(inputId, astFor(byOutput)(toolProducingInput))
    }
    
    ToolNode(tool.id, tool.spec, inputTrees)
  }
}
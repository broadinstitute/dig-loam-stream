package loamstream.model

import scala.util.Try

import loamstream.util.Tries
import loamstream.util.Maps

/**
 * @author clint
 * date: Apr 29, 2016
 *
 * Class representing the tree of relationships between tools in a pipeline.  Allows
 * composing trees and tools.
 */
final case class AST(toolSpec: ToolSpec, inputs: Set[AST]) {
  //TODO
  def id: LId = LId.newAnonId
  
  def isLeaf: Boolean = inputs.isEmpty

  def dependsOn(dependencies: AST*): AST = copy(inputs = inputs ++ dependencies)

  //NB: I had wanted to call this "andThen", but that can't be added as an extension method
  //to Seqs of Tools/ASTs, since Seqs are functions and inherit a different method named andThen.
  //I also considered "then", but that's now a reserved word. :\ -Clint
  def thenRun(other: AST): AST = other.dependsOn(this)

  def ~>(other: AST): AST = this.thenRun(other)

  //NB: Use dummy all-implicit param list to allow 2 overloads that otherwise would be the same after erasure
  def dependsOn(dependencies: Tool*)(implicit discriminator: Int = 1): AST = dependsOn(dependencies.map(AST(_)): _*)

  def thenRun(other: Tool): AST = AST(other).dependsOn(this)

  def ~>(other: Tool): AST = this.thenRun(other)
}

object AST {
  /*def apply(output: Store): AST = apply(output.spec)

  def apply(output: StoreSpec): AST = AST(output, Set.empty[AST])

  def apply(output: Store, inputs: Set[AST]): AST = AST(output.spec, inputs)*/

  def apply(tool: Tool): AST = AST(tool.spec, Set.empty)

  object Implicits {
    final implicit class IterableOps(val tools: Iterable[Tool]) extends AnyVal {
      def thenRun(other: AST): AST = other.dependsOn(tools.toSeq: _*)

      def ~>(other: AST): AST = thenRun(other)

      def thenRun(other: Tool): AST = other.toAST.dependsOn(tools.toSeq: _*)

      def ~>(other: Tool): AST = thenRun(other)
    }

    final implicit class ToolOps(val self: Tool) extends AnyVal {
      def toAST: AST = AST(self)

      def dependsOn(t: Tool): AST = toAST.dependsOn(t)

      def thenRun(t: Tool): AST = toAST.thenRun(t)

      def ~>(t: Tool): AST = thenRun(t)

      def dependsOn(other: AST): AST = toAST.dependsOn(other)

      def thenRun(other: AST): AST = toAST.thenRun(other)

      def ~>(other: AST): AST = thenRun(other)
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
        t.inputs.map(_.toTuple).forall { case (inputId, inputSpec) =>
          !otherTool.outputs.map(_.toTuple).exists { case (outputId, outputSpec) =>
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
    val toolsProducingInputs = for {
      input <- tool.inputs.toSet[Store]
      inputSpec = input.spec
      producer <- byOutput.get(inputSpec).toSet.flatten
    } yield producer

    AST(tool.spec, toolsProducingInputs.map(astFor(byOutput)))
  }
}
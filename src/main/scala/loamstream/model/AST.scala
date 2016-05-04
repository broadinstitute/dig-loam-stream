package loamstream.model

import scala.util.Try

import loamstream.util.Tries

/**
 * @author clint
 * date: Apr 29, 2016
 * 
 * Class representing the tree of relationships between tools in a pipeline.  Allows
 * composing trees and tools. 
 */
final case class AST(output: StoreSpec, inputs: Set[AST]) {
  def isLeaf: Boolean = inputs.isEmpty

  def dependsOn(dependencies: AST*): AST = copy(inputs = inputs ++ dependencies)

  //NB: I had wanted to call this "andThen", but that can't be added as an extension method
  //to Seqs of Tools/ASTs, since Seqs are functions and inherit a different method named andThen.
  //I also considered "then", but that's now a reserved word. :\ -Clint
  def thenRun(other: AST): AST = other.dependsOn(this)

  def ~>(other: AST): AST = this.thenRun(other)
  
  //NB: Use dummy all-implicit param list to allow 2 overloads that otherwise would be the same after erasure
  def dependsOn(dependencies: ToolSpec*)(implicit discriminator: Int = 1): AST = dependsOn(dependencies.map(AST(_)): _*)

  def thenRun(other: ToolSpec): AST = AST(other).dependsOn(this)

  def ~>(other: ToolSpec): AST = this.thenRun(other)
}

object AST {
  def apply(output: StoreSpec): AST = AST(output, Set.empty[AST])

  def apply(tool: ToolSpec): AST = apply(tool.output)

  object Implicits {
    final implicit class IterableOps(val tools: Iterable[ToolSpec]) extends AnyVal {
      def thenRun(other: AST): AST = other.dependsOn(tools.toSeq: _*)

      def ~>(other: AST): AST = thenRun(other)
      
      def thenRun(other: ToolSpec): AST = other.toAST.dependsOn(tools.toSeq: _*)

      def ~>(other: ToolSpec): AST = thenRun(other)
    }
    
    final implicit class ToolOps(val self: ToolSpec) extends AnyVal {
      def toAST: AST = AST(self)
      
      def dependsOn(t: ToolSpec): AST = toAST.dependsOn(t)
      
      def thenRun(t: ToolSpec): AST = toAST.thenRun(t)
      
      def ~>(t: ToolSpec): AST = thenRun(t)
      
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
        val byOutput: Map[StoreSpec, Set[ToolSpec]] = pipeline.tools.groupBy(_.output)

        astFor(byOutput)(terminal.output)
      }
    }
  }

  /**
   * Given an LPipeline, finds the "last" Tool, the one who's output isn't the input
   * of any other tools.  This Tool can be seen as producing the "output" of a
   * pipeline.
   */
  private[model] def findTerminalTool(pipeline: LPipeline): Try[ToolSpec] = {
    val tools = pipeline.tools

    val toolsAndOthers = tools.map(t => t -> (tools - t))

    def isNoOnesInput(tool: ToolSpec, others: Set[ToolSpec]): Boolean = {
      def inputsOf(t: ToolSpec) = t.inputs
      def outputOf(t: ToolSpec) = t.output

      others.forall(otherTool => !inputsOf(otherTool).contains(outputOf(tool)))
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
  private[model] def astFor(byOutput: Map[StoreSpec, Set[ToolSpec]])(toolOutput: StoreSpec): AST = {
    val toolOption = byOutput.get(toolOutput).flatMap(_.headOption)

    def toInteriorNode(tool: ToolSpec) = {
      val inputSpecs = tool.inputs.toSet

      AST(tool.output, inputSpecs.map(astFor(byOutput)))
    }

    toolOption.map(toInteriorNode).getOrElse(AST(toolOutput))
  }
}
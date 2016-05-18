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
sealed trait AST extends Iterable[AST] {
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
   * Returns an iterator that does a post-order traversal of this tree
   */
  override def iterator: Iterator[AST] = postOrder
  
  /**
   * Returns an iterator that does a post-order traversal of this tree; that is, 
   * this node's children (dependencies/inputs) are visited before this node.  
   */
  def postOrder: Iterator[AST] = childIterator(_.postOrder) ++ Iterator.single(this)
  
  /**
   * Returns an iterator that does a pre-order traversal of this tree; that is, 
   * this node is visited before its children (dependencies/inputs).  
   */
  def preOrder: Iterator[AST] = Iterator.single(this) ++ childIterator(_.preOrder)
  
  private def childIterator(iterationStrategy: AST => Iterator[AST]): Iterator[AST] = {
    inputs.iterator.map(_.producer).flatMap(iterationStrategy)
  }

  /**
   * Convenience method to print the tree for debugging
   */
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
}
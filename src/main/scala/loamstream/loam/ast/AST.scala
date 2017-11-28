package loamstream.loam.ast

import loamstream.util.Loggable
import loamstream.model.LId
import loamstream.model.Tool

/**
 * @author clint
 * date: May 12, 2016
 *
 * Class representing the tree of relationships between tools in a pipeline.  Allows
 * composing trees and tools.
 */
sealed trait AST extends Loggable { self =>
  val id: LId = LId.newAnonId

  import AST._

  def dependencies: Set[Connection]

  def withDependencies(inputs: Set[Connection]): AST

  final def dependsOn(connection: Connection): AST = {
    dependsOn(connection.inputId, connection.outputId, connection.producer)
  }
  
  final def dependsOn(inputId: LId, outputId: LId, producer: AST): AST = {
    withDependencies(dependencies + Connection(inputId, outputId, producer))
  }

  final def output(outputId: LId): NamedOutput = NamedOutput(outputId, this)

  final def apply(outputId: LId): NamedOutput = output(outputId)

  final def get(inputId: LId): InputHandle = InputHandle(inputId, this)
  //NB: Trying out connect(...).to(...) instead of get(...).from(...)
  final def connect(inputId: LId): InputHandle = get(inputId)
  
  final def isLeaf: Boolean = dependencies.isEmpty

  /**
   * Returns an iterator that does a post-order traversal of this tree
   */
  def iterator: Iterator[AST] = postOrder
  
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
    dependencies.iterator.map(_.producer).flatMap(iterationStrategy)
  }

  /**
   * Convenience method to print the tree for debugging
   */
  def print(indent: Int = 0, via: Option[LId] = None, doPrint: String => Any = debug(_)): Unit = {
    val indentString = s"^${"-" * indent}${via.map(v => s"($v)").getOrElse("")} "

    doPrint(s"$indentString$id")

    dependencies.foreach { case Connection(inputId, outputId, dep) => 
      dep.print(indent + 2, Option(outputId), doPrint) 
    }
  }

  def leaves: Set[AST] = {
    if (isLeaf) { Set(this) }
    else { dependencies.flatMap(_.producer.leaves) }
  }
}

object AST {
  def apply(tool: Tool): AST = ToolNode(tool)

  final case class ToolNode(
      override val id: LId,
      tool: Tool, 
      dependencies: Set[Connection] = Set.empty) extends AST {
    
    override def withDependencies(newDeps: Set[Connection]): AST = copy(dependencies = newDeps)
  }
  
  object ToolNode {
    def apply(tool: Tool, dependencies: Set[Connection]): ToolNode = ToolNode(tool.id, tool, dependencies)
    def apply(tool: Tool): ToolNode = apply(tool, Set.empty[Connection])
  }
  
  final case class Either(lhs: AST, rhs: AST, dependencies: Set[Connection] = Set.empty) extends AST {
    override def withDependencies(newDeps: Set[Connection]): AST = copy(dependencies = newDeps)
  }

  final case class Connection(inputId: LId, outputId: LId, producer: AST) {
    override def toString: String = s"($inputId) <~ ($outputId)$producer"
  }
  
  final case class ConsumerConnection(consumer: AST, inputId: LId, outputId: LId) {
    def from(producer: AST): AST = consumer.dependsOn(inputId, outputId, producer)
  }
  
  final case class InputHandle(inputId: LId, consumer: AST) {
    def from(namedOutput: NamedOutput): AST = consumer.dependsOn(inputId, namedOutput.outputId, namedOutput.producer)
    //NB: Trying out connect(...).to(...) instead of get(...).from(...)
    def to(namedOutput: NamedOutput): AST = from(namedOutput)
    
    def from(outputId: LId) = ConsumerConnection(consumer, inputId, outputId)
    //NB: Trying out connect(...).to(...) instead of get(...).from(...)
    def to(outputId: LId) = from(outputId)
  }
  
  final case class NamedOutput(outputId: LId, producer: AST) {
    def as(inputId: LId): Connection = Connection(inputId, outputId, producer)
  }
}

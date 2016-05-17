package loamstream.ideas

import loamstream.model.LId
import loamstream.model.ToolSpec
import loamstream.model.kinds.CompositeKind

/**
 * @author clint
 * date: May 12, 2016
 */
sealed trait NewAST {
  def id: LId = LId.newAnonId

  import NewAST._
  
  def inputs: Set[NamedInput]

  def withInputs(inputs: Set[NamedInput]): NewAST

  final def <~(input: NamedInput): NewAST = dependsOn(input)
  
  final def dependsOn(input: NamedInput): NewAST = dependsOn(input.id, input.producer)
  final def dependsOn(inputId: LId, input: NewAST): NewAST = withInputs(inputs + NamedInput(inputId, input))
  
  final def dependsOn(inputId: LId): NamedDependency = NamedDependency(this, inputId)
  
  final def dependsOn(inputId: LId, rest: LId*): MultiNamedDependency = MultiNamedDependency(this, rest.toSet + inputId)

  final def output(outputId: LId): NamedInput = NamedInput(outputId, this)

  final def apply(outputId: LId): NamedInput = output(outputId)

  final def isLeaf: Boolean = inputs.isEmpty

  /**
   * Performs post-order traversal and invokes f for each node
   */
  def traverse(f: NewAST => Any): Unit = {
    inputs.foreach(_.producer.traverse(f))
    
    f(this)
  }
  
  //TODO: TEST!
  def fold[R](z: R)(op: (R, NewAST) => R): R = {
    var acc = z
    
    traverse { node =>
      acc = op(acc, node)
    }
    
    acc
  }
  
  def print(indent: Int = 0, via: Option[LId] = None): Unit = {
    val indentString = s"${"-" * indent}${via.map(v => s"($v)").getOrElse("")}> "

    println(s"$indentString$id")

    inputs.foreach { case NamedInput(inputId, dep) => dep.print(indent + 2, Option(inputId)) }
  }
  
  def leaves: Set[NewAST] = {
    if(isLeaf) { Set(this) } 
    else { inputs.flatMap(_.producer.leaves) }
  }
}

object NewAST {
  final implicit class SeqOfASTsOps(val asts: Seq[NewAST]) extends AnyVal {
    def outputs(ids: LId.CompositeId*): Seq[NamedInput] = {
      val byId: Map[LId, NewAST] = asts.map(c => c.id -> c).toMap
      
      //NB: Can fail
      ids.collect { case LId.CompositeId(namespace, name) if byId.contains(namespace) => byId(namespace).output(name) }
    }
  }
  
  final implicit class SpecOps(val spec: ToolSpec) extends AnyVal {
    def as(id: LId): ToolNode = ToolNode(id, spec)
  }

  final case class NamedDependency(consumer: NewAST, outputId: LId) {
    def from(producer: NewAST): NewAST = consumer.dependsOn(outputId, producer)
  }

  final case class MultiNamedDependency(consumer: NewAST, outputIds: Set[LId]) {
    def from(producer: NewAST): NewAST = {
      outputIds.foldLeft(consumer) { (ast, id) =>
        ast.dependsOn(id).from(producer)
      }
    }
  }

  final case class NamedInput(id: LId, producer: NewAST)

  final case class ToolNode(override val id: LId, spec: ToolSpec, inputs: Set[NamedInput] = Set.empty) extends NewAST {
    override def withInputs(newInputs: Set[NamedInput]): NewAST = copy(inputs = newInputs)
  }

  final case class Either(lhs: NewAST, rhs: NewAST, inputs: Set[NamedInput] = Set.empty) extends NewAST {
    override def withInputs(newInputs: Set[NamedInput]): NewAST = copy(inputs = newInputs)
  }

  final case class Parallel(components: Seq[NewAST], inputs: Set[NamedInput] = Set.empty) extends NewAST {
    override val id = LId.LNamedId(components.map(_.id).mkString(","))

    private lazy val byId: Map[LId, NewAST] = components.map(c => c.id -> c).toMap

    override def withInputs(newInputs: Set[NamedInput]): NewAST = copy(inputs = newInputs)

    def outputs(ids: LId.CompositeId*): Seq[NamedInput] = {
      ids.map { case LId.CompositeId(namespace, name) => byId(namespace).output(name) }
    }
  }
}
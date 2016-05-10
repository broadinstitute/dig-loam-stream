package loamstream.ideas

import loamstream.model.LSig
import loamstream.model.kinds.LKind
import loamstream.model.LId
import loamstream.model.kinds.LAnyKind
import loamstream.model.values.LType
import loamstream.Sigs
import loamstream.model.kinds.StoreKinds
import cats.free.Free
import loamstream.model.LId.LNamedId

/**
 * @author clint
 * date: May 5, 2016
 */
object TraitBased extends App {

  sealed trait Step[+A]

  type Pipeline[A] = Free[Step, A]

  //Trying out kind-less 'StoreSpec' that's just a sig
  final case class ToolSpec( /*kind: LKind,*/ inputs: Map[LId, LSig], outputs: Map[LId, LSig])

  object ToolSpec {
    final implicit class SpecOps(val spec: ToolSpec) extends AnyVal {
      def as(id: LId): ToolNode = ToolNode(id, spec)
    }
  }

  sealed trait AST {
    def id: LId = LId.newAnonId

    def spec: ToolSpec

    def inputs: Set[NamedInput]

    def withInputs(inputs: Set[NamedInput]): AST

    def dependsOn(inputId: LId, input: AST): AST = withInputs(inputs + NamedInput(inputId, input))

    def dependsOn(inputId: LId): NamedDependency = NamedDependency(this, inputId)
    def dependsOn(inputId: LId, rest: LId*): MultiNamedDependency = MultiNamedDependency(this, rest.toSet + inputId)

    final def output(outputId: LId): NamedInput = NamedInput(outputId, this)

    final def apply(outputId: LId): NamedInput = output(outputId)

    final def isLeaf: Boolean = inputs.isEmpty

    def print(indent: Int = 0, via: Option[LId] = None): Unit = {
      val indentString = s"${"-" * indent}${via.map(v => s"($v)").getOrElse("")}> "

      println(s"$indentString$id")

      inputs.foreach { case NamedInput(inputId, dep) => dep.print(indent + 2, Option(inputId)) }
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

  final case class NamedInput(id: LId, ast: AST) {
    def to(consumer: AST): AST = consumer.dependsOn(id, ast)

    def ~>(consumer: AST): AST = to(consumer)

    def to(consumers: Seq[AST]): Parallel = Parallel(consumers.map(consumer => consumer.dependsOn(id, ast)))

    def ~>(consumers: Seq[AST]): Parallel = to(consumers)
  }

  final case class ToolNode(override val id: LId, spec: ToolSpec, inputs: Set[NamedInput] = Set.empty) extends AST {
    override def withInputs(newInputs: Set[NamedInput]): AST = copy(inputs = newInputs)
  }

  final case class Either(predicate: () => Boolean, lhs: AST, rhs: AST, inputs: Set[NamedInput] = Set.empty) extends AST {
    require(lhs.spec == rhs.spec)

    override def spec: ToolSpec = lhs.spec

    override def withInputs(newInputs: Set[NamedInput]): AST = copy(inputs = newInputs)
  }

  final case class Parallel(components: Seq[AST], inputs: Set[NamedInput] = Set.empty) extends AST {
    override val id = LId.LNamedId(components.map(_.id).mkString(","))

    private lazy val byId: Map[LId, AST] = components.map(c => c.id -> c).toMap

    override def spec: ToolSpec = {
      val z = ToolSpec(Map.empty, Map.empty)

      components.foldLeft(z) { (lhsSpec, rhs) =>
        val rhsSpec = rhs.spec

        ToolSpec(lhsSpec.inputs ++ rhsSpec.inputs, lhsSpec.outputs ++ rhsSpec.outputs)
      }
    }

    override def withInputs(newInputs: Set[NamedInput]): AST = copy(inputs = newInputs)

    def outputs(ids: LId.CompositeId*): Seq[NamedInput] = {
      ids.map { case LId.CompositeId(namespace, name) => byId(namespace).output(name) }
    }
  }

  object AST {
    final implicit class SeqOfNamedInputsOps(val asts: Seq[NamedInput]) extends AnyVal {
      def ~>(downstream: AST): AST = downstream.withInputs(asts.toSet)
    }
  }

  object StoreSigs {
    val genotypeCalls = Sigs.variantAndSampleToGenotype
  }

  import Ids._
  import Specs._

  {
    val a = aSpec as A

    val b = bSpec as B
    val c = cSpec as C
    val d = dSpec as D

    val h = hSpec as H
    val t = tSpec as T
    val u = uSpec as U
    val v = vSpec as V
    val e = eSpec as E
    val f = fSpec as F
    val g = gSpec as G
    val x = xSpec as X
    val y = ySpec as Y
    val z = zSpec as Z

    val e2t = e.output(E) ~> t

    val t2e = t.dependsOn(E, e)

    val alsoT2e = t.dependsOn(E).from(e)

    val ast = t.output(T) ~> (h.output(H) ~> z)

    import AST._

    {
      val bcd = Parallel(Seq(b, c, d))

      val a2x = a(A) ~> x

      val bcd2y = bcd.outputs(b.id / B, c.id / C, d.id / D) ~> y

      val a2y = a2x(X) ~> bcd2y

      val efg2tuv = Seq(e(E) ~> t, f(F) ~> u, g(G) ~> v)

      val a2tuv = a2y(Y) ~> efg2tuv

      val a2h = (a2tuv).outputs(t.id / T, u.id / U, v.id / V) ~> h

      val a2z = a2h(H) ~> z

      //a2x.print()

      //println()

      //bcd2y.print()

      //println()

      //a2y.print()
    }
    
    {
      val a2x = x.dependsOn(A).from(a)
      
      val a2bcd = Parallel(Seq(b.dependsOn(X).from(x), c.dependsOn(Y).from(y), d.dependsOn(Z).from(z)))
      
      val a2y = y.dependsOn(b.id / B, c.id / C, d.id / D).from(a2bcd)
      
      a2y.print()
    }
  }

  object Specs {
    private val kind = LAnyKind

    import LType._

    private val hSig = LInt to LDouble
    private val zSig = hSig
    private val sig = hSig

    val zSpec = ToolSpec(inputs = Map(H -> hSig), outputs = Map(Z -> zSig))

    val hSpec = ToolSpec(inputs = Map(T -> sig, U -> sig, V -> sig), outputs = Map(H -> hSig))

    val tSpec = ToolSpec(inputs = Map(E -> sig), outputs = Map(T -> sig))
    val uSpec = ToolSpec(inputs = Map(F -> sig), outputs = Map(U -> sig))
    val vSpec = ToolSpec(inputs = Map(G -> sig), outputs = Map(V -> sig))

    val eSpec = ToolSpec(inputs = Map(Y -> sig), outputs = Map(E -> sig))
    val fSpec = ToolSpec(inputs = Map(Y -> sig), outputs = Map(F -> sig))
    val gSpec = ToolSpec(inputs = Map(Y -> sig), outputs = Map(G -> sig))

    val xSpec = ToolSpec(inputs = Map(A -> sig), outputs = Map(X -> sig))
    val ySpec = ToolSpec(inputs = Map(), outputs = Map(E -> sig, F -> sig, G -> sig))

    val aSpec = ToolSpec(inputs = Map(), outputs = Map(A -> sig))

    val bSpec = ToolSpec(inputs = Map(X -> sig), outputs = Map(B -> sig))
    val cSpec = ToolSpec(inputs = Map(X -> sig), outputs = Map(C -> sig))
    val dSpec = ToolSpec(inputs = Map(X -> sig), outputs = Map(D -> sig))
  }

  object Ids {
    val A = LId.LNamedId("A")
    val B = LId.LNamedId("B")
    val C = LId.LNamedId("C")
    val D = LId.LNamedId("D")
    val E = LId.LNamedId("E")
    val F = LId.LNamedId("F")
    val G = LId.LNamedId("G")
    val H = LId.LNamedId("H")
    val I = LId.LNamedId("I")
    val J = LId.LNamedId("J")
    val K = LId.LNamedId("K")
    val L = LId.LNamedId("L")
    val M = LId.LNamedId("M")
    val N = LId.LNamedId("N")
    val O = LId.LNamedId("O")
    val P = LId.LNamedId("P")
    val Q = LId.LNamedId("Q")
    val R = LId.LNamedId("R")
    val S = LId.LNamedId("S")
    val T = LId.LNamedId("T")
    val U = LId.LNamedId("U")
    val V = LId.LNamedId("V")
    val W = LId.LNamedId("W")
    val X = LId.LNamedId("X")
    val Y = LId.LNamedId("Y")
    val Z = LId.LNamedId("Z")
  }
}
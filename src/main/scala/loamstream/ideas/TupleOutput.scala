package loamstream.ideas

import loamstream.model.LSig
import loamstream.model.kinds.LKind
import loamstream.model.LId
import loamstream.model.kinds.LAnyKind
import loamstream.model.values.LType
import loamstream.Sigs
import loamstream.model.kinds.StoreKinds

/**
 * @author clint
 * date: May 5, 2016
 */
object Trees {

  //Trying out kind-less 'StoreSpec' that's just a sig

  final case class ToolSpec(kind: LKind, inputs: Map[LId, LSig], outputs: Map[LId, LSig])

  object ToolSpec {
    final implicit class SpecOps(val spec: ToolSpec) extends AnyVal {
      def as(id: LId): AST = AST(id, spec)
    }
  }

  final case class AST(id: LId, spec: ToolSpec, inputs: Seq[AST] = Seq.empty) {
    def isLeaf: Boolean = inputs.isEmpty

    def dependsOn(dependencies: AST*): AST = copy(inputs = inputs ++ dependencies)

    //NB: I had wanted to call this "andThen", but that can't be added as an extension method
    //to Seqs of Tools/ASTs, since Seqs are functions and inherit a different method named andThen.
    //I also considered "then", but that's now a reserved word. :\ -Clinid: LId, inputs: Seq[Execution]t
    def thenRun(other: AST): AST = other.dependsOn(this)

    def ~>(other: AST): AST = this.thenRun(other)

    def ~>(others: Seq[AST]): Seq[AST] = others.map(other => this.thenRun(other))
  }

  object AST {
    /*final implicit class ASTOps(val ast: AST) extends AnyVal {
      
    }*/
    
    final implicit class SeqOfASTsOps(val asts: Seq[AST]) extends AnyVal {
      def ~>(other: AST): AST = other.dependsOn(asts: _*)
      
      def ~>(others: Seq[AST]): Seq[AST] = {
        asts.zip(others).map { case (ast, other) => ast thenRun other }
      }
    }
  }

  object Ids {
    val A = LId.newAnonId
    val B = LId.newAnonId
    val C = LId.newAnonId
    val D = LId.newAnonId
    val E = LId.newAnonId
    val F = LId.newAnonId
    val G = LId.newAnonId
    val H = LId.newAnonId
    val I = LId.newAnonId
    val J = LId.newAnonId
    val T = LId.newAnonId
    val U = LId.newAnonId
    val V = LId.newAnonId
    val X = LId.newAnonId
    val Y = LId.newAnonId
    val Z = LId.newAnonId
  }

  object StoreSigs {
    val genotypeCalls = Sigs.variantAndSampleToGenotype
  }

  {
    val kind = LAnyKind

    import LType._
    import Ids._

    val hSig = LInt to LDouble
    val zSig = hSig
    val sig = hSig

    val zSpec = ToolSpec(kind = kind, inputs = Map(H -> hSig), outputs = Map(Z -> zSig))

    val hSpec = ToolSpec(kind = kind, inputs = Map(T -> sig, U -> sig, V -> sig), outputs = Map(H -> hSig))

    val tSpec = ToolSpec(kind = kind, inputs = Map(E -> sig), outputs = Map(T -> sig))
    val uSpec = ToolSpec(kind = kind, inputs = Map(F -> sig), outputs = Map(U -> sig))
    val vSpec = ToolSpec(kind = kind, inputs = Map(G -> sig), outputs = Map(V -> sig))

    val eSpec = ToolSpec(kind = kind, inputs = Map(Y -> sig), outputs = Map(E -> sig))
    val fSpec = ToolSpec(kind = kind, inputs = Map(Y -> sig), outputs = Map(F -> sig))
    val gSpec = ToolSpec(kind = kind, inputs = Map(Y -> sig), outputs = Map(G -> sig))

    val ySpec = ToolSpec(kind = kind, inputs = Map(), outputs = Map(E -> sig, F -> sig, G -> sig))

    val efg2tuv: Seq[AST] = Seq(
        (eSpec as E) ~> (tSpec as T), 
        (fSpec as E) ~> (uSpec as T), 
        (gSpec as E) ~> (vSpec as T))
    
    val ast = (ySpec as Y) ~> efg2tuv ~> (hSpec as H) ~> (zSpec as Z)
  }

  trait Reified[S] extends LId.Owner {
    def spec: S
  }

  object Reified {
    final case class Store(id: LId, spec: LSig) extends Reified[LSig]

    final case class Tool(id: LId, spec: ToolSpec) extends Reified[ToolSpec]
  }

  trait Execution extends LId.Owner {
    def inputs: Seq[Execution]
  }

  final case class ExecutionNode(id: LId, inputs: Seq[Execution])

  final case class CompositeExecution(id: LId, inputs: Seq[Execution])
}
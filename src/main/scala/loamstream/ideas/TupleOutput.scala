/*package loamstream.ideas

import loamstream.model.LSig
import loamstream.model.kinds.LKind
import loamstream.model.LId
import loamstream.model.kinds.LAnyKind
import loamstream.model.values.LType
import loamstream.Sigs
import loamstream.model.kinds.StoreKinds

*//**
 * @author clint
 * date: May 5, 2016
 *//*
object Trees {

  //Trying out kind-less 'StoreSpec' that's just a sig

  final case class ToolSpec(kind: LKind, inputs: Map[LId, LSig], outputs: Map[LId, LSig])

  object ToolSpec {
    final implicit class SpecOps(val spec: ToolSpec) extends AnyVal {
      def as(id: LId): AST = AST(id, spec)
    }
  }

  final case class NamedOutput(producer: AST, outputId: LId) {
    def to(consumer: AST): AST = consumer.dependsOn(outputId, producer)
    
    def ~>(consumer: AST): AST = to(consumer)
    
    def to(consumers: Seq[NamedOutput]): Seq[AST] = consumers.map { consumer =>
      val producer = consumer.producer
      
      producer.connect(outputId).to(producer))
    }
    
    def ~>(consumers: Seq[NamedOutput]): Seq[AST] = to(consumers)
    
    def modify(f: AST => AST): NamedOutput = copy(f(producer), outputId)
  }
  
  final case class AST(id: LId, spec: ToolSpec, inputs: Map[LId, AST] = Map.empty) {
    def isLeaf: Boolean = inputs.isEmpty

    def dependsOn(inputId: LId, input: AST): AST = dependsOn(inputId -> input)
    
    def dependsOn(dependencies: (LId, AST)*): AST = copy(inputs = inputs ++ dependencies)

    def connect(outputId: LId): NamedOutput = NamedOutput(this, outputId)
    
    def output(outputId: LId): NamedOutput = connect(outputId)
  }

  object AST {
    final implicit class ASTOps(val ast: AST) extends AnyVal {
      
    }
    
    final implicit class SeqOfASTsOps(val asts: Seq[AST]) extends AnyVal {
      def ~>(other: AST): AST = other.dependsOn(asts: _*)
      
      def ~>(others: Seq[AST]): Seq[AST] = {
        //TODO: What if one seq is longer than the other? Will probably want to pass through "ends" of seqs.  But which one?  How to align seqs?  Will have to match on inputs/outputs.
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

    //val efg2tuv: Seq[AST] = 
    
    val e2t = ((eSpec as E).output(E) ~> (tSpec as T)).output(outputId)
    
    val ast = (ySpec as Y).connect(Y) ~> Seq((eSpec as E) ~> (tSpec as T), 
                                  (fSpec as F) ~> (uSpec as U), 
                                  (gSpec as G) ~> (vSpec as V)) ~> (hSpec as H) ~> (zSpec as Z)
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
}*/
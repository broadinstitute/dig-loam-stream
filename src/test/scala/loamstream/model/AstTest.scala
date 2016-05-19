package loamstream.model

import org.scalatest.FunSuite

import loamstream.apps.hail.HailPipeline
import loamstream.apps.minimal.MiniPipeline
import loamstream.pipelines.qc.ancestry.AncestryInferencePipeline
import loamstream.util.Maps
import LId.LNamedId
import loamstream.model.values.LType
import loamstream.model.kinds.LKind
import loamstream.model.kinds.LAnyKind

/**
 * @author clint
 * date: May 2, 2016
 */
final class AstTest extends FunSuite {
  import AST._
  import Ids._
  import Tools._
  import Nodes._

  test("leaves()") {
    assert(a.leaves == Set(a))

    assert(a.dependsOn(b(B).as(X)).leaves == Set(b))

    assert(a.dependsOn(b(B).as(X)).dependsOn(c(C).as(Y)).leaves == Set(b, c))

    //a -> b -> (c, d)

    assert(Trees.abcd.leaves == Set(c, d))
  }

  test("isLeaf") {
    assert(a.isLeaf == true)

    assert(a.dependsOn(b(B).as(X)).isLeaf == false)

    assert(Trees.abcd.isLeaf == false)
    assert(Trees.bcd.isLeaf == false)
    assert(c.isLeaf == true)
    assert(d.isLeaf == true)
  }

  private def doTraversalTest(ast: AST, iteratorFrom: AST => Iterator[AST], validate: Seq[LId] => Unit): Unit = {
    
    def idsFrom(asts: Iterator[AST]): Seq[LId] = {
      asts.map(_.id).toIndexedSeq
    }
    
    {
      //just node a

      val visited = idsFrom(iteratorFrom(a))

      assert(visited == Seq(A))
    }

    val visited = idsFrom(iteratorFrom(ast))

    validate(visited)
  }
  
  test("postOrder()") {
    doTraversalTest(Trees.abcd, _.postOrder, visited => {
      assert(visited.take(2).toSet == Set(C, D))

      assert(visited.drop(2) == Seq(B, A))
    })
  }
  
  test("iterator()") {
    doTraversalTest(Trees.abcd, _.iterator, visited => {
      assert(visited.take(2).toSet == Set(C, D))

      assert(visited.drop(2) == Seq(B, A))
    })
  }
  
  test("preOrder()") {
    doTraversalTest(Trees.abcd, _.preOrder, visited => {
      assert(visited.take(2) == Seq(A, B))

      assert(visited.drop(2).toSet == Set(C, D))
    })
  }
  
  test(s"1 node dependsOn 1 other node (${classOf[NamedOutput].getSimpleName}) => AST") {
    val ast = a dependsOn (b(Z) as Q)

    val expected = ToolNode(A, a.tool, Set(Connection(Q, Z, b)))

    assert(ast == expected)
  }
  
  test("1 node dependsOn 1 other node (id, ast) => AST") {
    val ast = a.dependsOn(Q, Z, b)

    val expected = ToolNode(A, a.tool, Set(Connection(Q, Z, b)))

    assert(ast == expected)
  }
  
  test("1 node dependsOn 1 other node (connection) => AST") {
    val connection = Connection(A, B, b)
    
    val ast = a.dependsOn(connection)

    val expected = ToolNode(A, a.tool, Set(connection))

    assert(ast == expected)
  }

  test("1 node dependsOn 1 other node get(id).from(named dep)") {
    val ast = a.get(Z).from(b(X))

    val expected = ToolNode(A, a.tool, Set(Connection(Z, X, b)))

    assert(ast == expected)
  }
  
  test("1 node dependsOn 1 other node get(iid).from(oid).from(producer)") {
    val ast = a.get(Z).from(X).from(b)

    val expected = ToolNode(A, a.tool, Set(Connection(Z, X, b)))

    assert(ast == expected)
  }

  test("output(LId) and apply(LId)") {
    assert(a.output(Z) == NamedOutput(Z, a))

    assert(a(Z) == NamedOutput(Z, a))
  }

  test("'Complex' pipeline") {
    /*
     *            b
     *          /   \
     *  a <- x <- c  <- y
     *          \   /
     *            d
     */
    
    val b2y = b.dependsOn(y(Y).as(I))
    val c2y = c.dependsOn(y(Y).as(I))
    val d2y = d.dependsOn(y(Y).as(I))
    
    val bcd = Set(b2y.output(B).as(B), c2y.output(C).as(C), d2y.output(D).as(D))
    
    val x2bcd = x.withDependencies(bcd)
    
    val a2y = a.dependsOn(x2bcd(X).as(I))

    val expected = {
      a.dependsOn(I, X, x.withDependencies {
        Set(
          b.dependsOn(y(Y).as(I)).output(B).as(B), 
          c.dependsOn(y(Y).as(I)).output(C).as(C), 
          d.dependsOn(y(Y).as(I)).output(D).as(D))
      })
    }
    
    assert(a2y == expected)
    
    def getChildOf(root: AST, childName: LId, rest: LId*): AST = {
      def childWithId(rt: AST, id: LId): AST = {
        rt.dependencies.find(_.outputId == id).get.producer
      }
      
      val z: AST = childWithId(root, childName)
      
      rest.foldLeft(z)(childWithId)
    }
    
    assert(a2y.dependencies.size == 1)
    
    assert(getChildOf(a2y, X).dependencies.size == 3)
    assert(getChildOf(a2y, X, B).dependencies.size == 1)
    assert(getChildOf(a2y, X, C).dependencies.size == 1)
    assert(getChildOf(a2y, X, D).dependencies.size == 1)
    
    assert(getChildOf(a2y, X, B, Y).dependencies.size == 0)
    assert(getChildOf(a2y, X, C, Y).dependencies.size == 0)
    assert(getChildOf(a2y, X, D, Y).dependencies.size == 0)
    
    val visitCounts: Map[LId, Int] = {
      a2y.iterator.map(_.id).toIndexedSeq.groupBy(identity).mapValues(_.size)
    }
    
    val expectedCounts = Map(
        A -> 1,
        X -> 1,
        B -> 1,
        C -> 1,
        D -> 1,
        Y -> 3)
    
    assert(visitCounts == expectedCounts)
  }

  private object Trees {
    //a -> b -> (c, d)

    lazy val bcd = b.dependsOn(c(C).as(I)).dependsOn(d(D).as(J))

    lazy val abcd = a.get(B).from(I).from(bcd)
  }
  
  private object Nodes {
    val a = ToolNode(SimpleTool(aSpec, A))

    val b = ToolNode(SimpleTool(bSpec, B))
    val c = ToolNode(SimpleTool(cSpec, C))
    val d = ToolNode(SimpleTool(dSpec, D))

    val h = ToolNode(SimpleTool(hSpec, H))
    val t = ToolNode(SimpleTool(tSpec, T))
    val u = ToolNode(SimpleTool(uSpec, U))
    val v = ToolNode(SimpleTool(vSpec, V))
    val e = ToolNode(SimpleTool(eSpec, E))
    val f = ToolNode(SimpleTool(fSpec, F))
    val g = ToolNode(SimpleTool(gSpec, G))
    val x = ToolNode(SimpleTool(xSpec, X))
    val y = ToolNode(SimpleTool(ySpec, Y))
    val z = ToolNode(SimpleTool(zSpec, Z))
  }

  private object Tools {
    import LType._

    final case class SimpleStore(spec: StoreSpec, id: LId = LId.newAnonId) extends Store
    
    final case class SimpleTool(spec: ToolSpec, id: LId = LId.newAnonId) extends Tool {
      private def toStoreMap(m: Map[LId, StoreSpec]): Map[LId, Store] = m.mapValues(SimpleStore(_))
      
      override val inputs: Map[LId, Store] = toStoreMap(spec.inputs)
  
      override val outputs: Map[LId, Store] = toStoreMap(spec.outputs)
    }
    
    //NB: These specs are all totally bogus, and are basically placeholders just to have a way to make unique nodes.
    //That's fine for now since we don't 'typecheck' ASTs.  This will change in the near future.
    
    private val kind: LKind = LAnyKind

    private val hStoreSpec = StoreSpec(LInt to LDouble, kind)
    private val zStoreSpec = hStoreSpec
    private val storeSpec = hStoreSpec

    val zSpec = ToolSpec(kind, inputs = Map(H -> hStoreSpec), outputs = Map(Z -> zStoreSpec))

    val hSpec = ToolSpec(kind, inputs = Map(T -> storeSpec, U -> storeSpec, V -> storeSpec), outputs = Map(H -> hStoreSpec))

    val tSpec = ToolSpec(kind, inputs = Map(E -> storeSpec), outputs = Map(T -> storeSpec))
    val uSpec = ToolSpec(kind, inputs = Map(F -> storeSpec), outputs = Map(U -> storeSpec))
    val vSpec = ToolSpec(kind, inputs = Map(G -> storeSpec), outputs = Map(V -> storeSpec))

    val eSpec = ToolSpec(kind, inputs = Map(Y -> storeSpec), outputs = Map(E -> storeSpec))
    val fSpec = ToolSpec(kind, inputs = Map(Y -> storeSpec), outputs = Map(F -> storeSpec))
    val gSpec = ToolSpec(kind, inputs = Map(Y -> storeSpec), outputs = Map(G -> storeSpec))

    val xSpec = ToolSpec(kind, inputs = Map(A -> storeSpec), outputs = Map(X -> storeSpec))
    val ySpec = ToolSpec(kind, inputs = Map(), outputs = Map(E -> storeSpec, F -> storeSpec, G -> storeSpec))

    val aSpec = ToolSpec(kind, inputs = Map(), outputs = Map(A -> storeSpec))

    val bSpec = ToolSpec(kind, inputs = Map(X -> storeSpec), outputs = Map(B -> storeSpec))
    val cSpec = ToolSpec(kind, inputs = Map(X -> storeSpec), outputs = Map(C -> storeSpec))
    val dSpec = ToolSpec(kind, inputs = Map(X -> storeSpec), outputs = Map(D -> storeSpec))
  }

  private object Ids {
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
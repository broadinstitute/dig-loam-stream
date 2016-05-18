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
  import Specs._
  import Nodes._

  test("leaves()") {
    assert(a.leaves == Set(a))

    assert(a.dependsOn(b(B)).leaves == Set(b))

    assert(a.dependsOn(b(B)).dependsOn(c(C)).leaves == Set(b, c))

    //a -> b -> (c, d)

    assert(Trees.abcd.leaves == Set(c, d))
  }

  test("isLeaf") {
    assert(a.isLeaf == true)

    assert(a.dependsOn(b(B)).isLeaf == false)

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
  
  test(s"1 node dependsOn 1 other node (${classOf[NamedInput].getSimpleName}) => AST") {
    val ast = a dependsOn b(Z)

    val expected = ToolNode(A, aSpec, Set(NamedInput(Z, b)))

    assert(ast == expected)
  }
  
  test(s"1 node <~ 1 other node (${classOf[NamedInput].getSimpleName}) => AST") {
    val ast = a <~ b(Z)

    val expected = ToolNode(A, aSpec, Set(NamedInput(Z, b)))

    assert(ast == expected)
  }

  test("1 node dependsOn 1 other node (id, ast) => AST") {
    val ast = a.dependsOn(Z, b)

    val expected = ToolNode(A, aSpec, Set(NamedInput(Z, b)))

    assert(ast == expected)
  }

  test("1 node dependsOn 1 other node (id) => named dep") {
    val namedDep = a.dependsOn(Z)

    val expected = NamedDependency(a, Z)

    assert(namedDep == expected)
  }

  test("1 node dependsOn 1 other node (id) => named dep => ast => ast") {
    val namedDep = a.dependsOn(Z).from(b)

    val expected = ToolNode(A, aSpec, Set(NamedInput(Z, b)))

    assert(namedDep == expected)
  }

  test("1 node dependsOn 1 other node (id, id ... ) => multi named dep") {
    val namedDep = a.dependsOn(Z, Y, X)

    val expected = MultiNamedDependency(a, Set(Z, Y, X))

    assert(namedDep == expected)
  }

  test("1 node dependsOn 1 other node (id, id ... ) => multi named dep => ast => ast") {
    val namedDep = a.dependsOn(Z, Y, X).from(b)

    val expected = ToolNode(A, aSpec, Set(NamedInput(Z, b), NamedInput(Y, b), NamedInput(X, b)))

    assert(namedDep == expected)
  }

  test("output(LId) and apply(LId)") {
    assert(a.output(Z) == NamedInput(Z, a))

    assert(a(Z) == NamedInput(Z, a))
  }

  test("'Complex' pipeline") {
    /*
     *            b
     *          /   \
     *  a <- x <- c  <- y
     *          \   /
     *            d
     */
    
    val b2y = b.dependsOn(y(Y))
    val c2y = c.dependsOn(y(Y))
    val d2y = d.dependsOn(y(Y))
    
    val bcd = Set(b2y.output(B), c2y.output(C), d2y.output(D))
    
    val x2bcd = x.withInputs(bcd)
    
    val a2y = a.dependsOn(X, x2bcd)

    val expected = {
      a.dependsOn(X, x.withInputs {
        Set(b.dependsOn(y(Y)).output(B), c.dependsOn(y(Y)).output(C), d.dependsOn(y(Y)).output(D))
      })
    }
    
    assert(a2y == expected)
    
    def getChildOf(root: AST, childName: LId, rest: LId*): AST = {
      def childWithId(rt: AST, id: LId): AST = {
        rt.inputs.find(_.id == id).get.producer
      }
      
      val z: AST = childWithId(root, childName)
      
      rest.foldLeft(z)(childWithId)
    }
    
    assert(a2y.inputs.size == 1)
    
    assert(getChildOf(a2y, X).inputs.size == 3)
    assert(getChildOf(a2y, X, B).inputs.size == 1)
    assert(getChildOf(a2y, X, C).inputs.size == 1)
    assert(getChildOf(a2y, X, D).inputs.size == 1)
    
    assert(getChildOf(a2y, X, B, Y).inputs.size == 0)
    assert(getChildOf(a2y, X, C, Y).inputs.size == 0)
    assert(getChildOf(a2y, X, D, Y).inputs.size == 0)
    
    val z = Map.empty[LId, Int]
    
    val visitCounts = a2y.iterator.foldLeft(z) { (visitCounts, node) =>
      val newCount = visitCounts.getOrElse(node.id, 0) + 1
      
      visitCounts + (node.id -> newCount)
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

    lazy val bcd = b.dependsOn(c(C)).dependsOn(d(D))

    lazy val abcd = a.dependsOn(B).from(bcd)
  }
  
  private object Nodes {
    import AST.Implicits._
    
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
  }

  private object Specs {
    import LType._

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
package loamstream.ideas

import org.scalatest.FunSuite
import loamstream.model.LId
import loamstream.model.values.LType
import loamstream.Sigs
import loamstream.model.kinds.LAnyKind
import loamstream.model.kinds.LKind
import loamstream.model.StoreSpec

/**
 * @author clint
 * date: May 12, 2016
 */
final class NewAstTest extends FunSuite {
  
  import Ids._
  import Specs._
  import NewAST._
  import Nodes._

  test("leaves()") {
    assert(a.leaves == Set(a))
    
    assert(a.dependsOn(b(B)).leaves == Set(b))
    
    assert(a.dependsOn(b(B)).dependsOn(c(C)).leaves == Set(b, c))
    
    //a -> b -> (c, d)
    
    val bc = b.dependsOn(c(C))
    val bcd = bc.dependsOn(d(D))
    
    val abcd = a.dependsOn(B).from(bcd)
    
    assert(abcd.leaves == Set(c,d))
  }
  
  test(s"1 node dependsOn 1 other node (${classOf[NamedInput].getSimpleName}) => AST") {
    val ast = a dependsOn b(Z)
    
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

  object Nodes {
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

    import NewAST._

    {
      val bcd = Parallel(Seq(b, c, d))

      val a2x = a(A) ~> x

      val bcd2y = bcd.outputs(b.id / B, c.id / C, d.id / D) ~> y

      val a2y = a2x(X) ~> bcd2y

      val efg2tuv = Seq(e(E) ~> t, f(F) ~> u, g(G) ~> v)

      val a2tuv = a2y(Y) ~> efg2tuv

      val a2h = (a2tuv).outputs(t.id / T, u.id / U, v.id / V) ~> h

      val a2z = a2h(H) ~> z
    }
    
    {
      val a2x = x.dependsOn(A).from(a)
      
      val a2bcd = Parallel(Seq(b.dependsOn(X).from(x), c.dependsOn(Y).from(y), d.dependsOn(Z).from(z)))
      
      val a2y = y.dependsOn(b.id / B, c.id / C, d.id / D).from(a2bcd)
      
      //a2y.print()
    }
  }

  object Specs {
    import LType._

    private val kind: LKind = LAnyKind
    
    private val hStoreSpec = StoreSpec(LInt to LDouble, kind)
    private val zStoreSpec = hStoreSpec
    private val storeSpec = hStoreSpec

    val zSpec = AstSpec(inputs = Map(H -> hStoreSpec), outputs = Map(Z -> zStoreSpec))

    val hSpec = AstSpec(inputs = Map(T -> storeSpec, U -> storeSpec, V -> storeSpec), outputs = Map(H -> hStoreSpec))

    val tSpec = AstSpec(inputs = Map(E -> storeSpec), outputs = Map(T -> storeSpec))
    val uSpec = AstSpec(inputs = Map(F -> storeSpec), outputs = Map(U -> storeSpec))
    val vSpec = AstSpec(inputs = Map(G -> storeSpec), outputs = Map(V -> storeSpec))

    val eSpec = AstSpec(inputs = Map(Y -> storeSpec), outputs = Map(E -> storeSpec))
    val fSpec = AstSpec(inputs = Map(Y -> storeSpec), outputs = Map(F -> storeSpec))
    val gSpec = AstSpec(inputs = Map(Y -> storeSpec), outputs = Map(G -> storeSpec))

    val xSpec = AstSpec(inputs = Map(A -> storeSpec), outputs = Map(X -> storeSpec))
    val ySpec = AstSpec(inputs = Map(), outputs = Map(E -> storeSpec, F -> storeSpec, G -> storeSpec))

    val aSpec = AstSpec(inputs = Map(), outputs = Map(A -> storeSpec))

    val bSpec = AstSpec(inputs = Map(X -> storeSpec), outputs = Map(B -> storeSpec))
    val cSpec = AstSpec(inputs = Map(X -> storeSpec), outputs = Map(C -> storeSpec))
    val dSpec = AstSpec(inputs = Map(X -> storeSpec), outputs = Map(D -> storeSpec))
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
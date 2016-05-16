package loamstream.ideas

import org.scalatest.FunSuite
import loamstream.model.LId
import loamstream.model.values.LType
import loamstream.Sigs
import loamstream.model.kinds.LAnyKind
import loamstream.model.kinds.LKind
import loamstream.model.StoreSpec
import loamstream.model.ToolSpec

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
  
  test("isLeaf") {
    assert(a.isLeaf == true)
    
    assert(a.dependsOn(b(B)).isLeaf == false)
    
    //a -> b -> (c, d)
      
    val bcd = b.dependsOn(c(C)).dependsOn(d(D))
    
    val abcd = a.dependsOn(B).from(bcd)
    
    assert(abcd.isLeaf == false)
    assert(bcd.isLeaf == false)
    assert(c.isLeaf == true)
    assert(d.isLeaf == true)
  }
  
  test("traverse()") {
    {
      //just node a
      
      var visited: Seq[NewAST] = Vector.empty
      
      assert(visited == Nil)
      
      a.traverse(visited :+= _)
      
      assert(visited == Seq(a))
    }
    
    {
      //a -> b -> (c, d)
      
      var visited: Seq[NewAST] = Vector.empty
      
      val bcd = b.dependsOn(c(C)).dependsOn(d(D))
    
      val abcd = a.dependsOn(B).from(bcd)
      
      abcd.traverse(visited :+= _)
      
      assert(visited.take(2).toSet == Set(c, d))
      
      assert(visited.drop(2) == Seq(bcd, abcd))
    }
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
  
  test("output(LId) and apply(LId)") {
    assert(a.output(Z) == NamedInput(Z, a))
    
    assert(a(Z) == NamedInput(Z, a))
  }
  
  test("NamedInput.to(AST) and NamedInput.~>(AST)") {
    //a depends on b
    val b2a = NamedInput(Z, b).to(a)
    
    assert(b(Z) ~> a === b2a)
    
    val expected1 = a.copy(inputs = Set(NamedInput(Z, b)))
    
    assert(b2a == expected1)
    
    assert(b(Z).to(a) == expected1)
    assert(b(Z) ~> (a) == expected1)
    
    //c depends on (a depends on b)
    
    val b2a2c = NamedInput(Y, b2a).to(c)
    
    assert(b2a(Y) ~> c === b2a2c)
    
    val expected2 = c.copy(inputs = Set(NamedInput(Y, a.copy(inputs = Set(NamedInput(Z, b))))))
    
    assert(b2a2c == expected2)
    
    assert(b2a(Y).to(c) == expected2)
    assert(b2a(Y) ~> (c) == expected2)
  }
  
  test("NamedInput.to(NamedInput) and NamedInput.~>(NamedInput)") {
    
    //c <- b <- a
    
    val a2b2c = a(A) to b(B) to c  
    
    assert(a2b2c == a(A).to(b(B)).to(c))
    
    assert(a2b2c == a(A).~>(b(B)).~>(c))
    
    assert(a2b2c == a(A) ~> b(B) ~> c)

    val expected = c.dependsOn(B, b.dependsOn(A, a))
    
    assert(a2b2c == expected)
  }
  
  test("NamedInput.to(Seq[AST]) and NamedInput.~>(Seq[AST])") {

    /*      a
     *    /
     *  x - b
     *    \
     *      c
     */
    
    val dests = Seq(a, b, c)
                    
    val asts = x(X).to(dests)
    
    assert(asts == x(X) ~> dests)
    
    val expected = Seq(x(X) ~> a,
                       x(X) ~> b,
                       x(X) ~> c)
    
    assert(asts == expected)
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

    //val ast = t.output(T) ~> (h.output(H) ~> z)

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
    }
  }

  object Specs {
    import LType._

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
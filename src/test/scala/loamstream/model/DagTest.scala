package loamstream.model

import org.scalatest.FunSuite

import scala.reflect.runtime.universe.typeOf


/**
  * @author clint
  *         date: May 2, 2016
  */
final class DagTest extends FunSuite {

  import DAG._
  import Ids._
  import Nodes._
  import Tools._

  test("leaves()") {
    assert(a.leaves == Set(a))

    assert(a.gets(X).from(b.output(B)).leaves == Set(b))

    assert(a.gets(X).from(b.output(B)).gets(Y).from(c.output(C)).leaves == Set(b, c))

    //a -> b -> (c, d)

    assert(Trees.abcd.leaves == Set(c, d))
  }  

  test("isLeaf") {
    assert(a.isLeaf === true)

    assert(a.gets(X).from(b.output(B)).isLeaf === false)

    assert(Trees.abcd.isLeaf === false)
    assert(Trees.bcd.isLeaf === false)
    assert(c.isLeaf === true)
    assert(d.isLeaf === true)
  }

  private def doTraversalTest(ast: DAG, iteratorFrom: DAG => Iterator[DAG], validate: Seq[String] => Unit): Unit = {

    import Helpers.dagNodeToString
    
    def stringsFrom(asts: Iterator[DAG]): Seq[String] = {
      asts.map(dagNodeToString).toIndexedSeq
    }

    {
      //just node a

      val visited = stringsFrom(iteratorFrom(a))

      assert(visited == Seq("Tool(A)"))
    }

    val visited = stringsFrom(iteratorFrom(ast))

    validate(visited)
  }

  private object Helpers {
    def tool(id: LId): String = s"Tool($id)"
    def store(id: LId): String = s"Store($id)"
    
    def dagNodeToString(d: DAG): String = d match {
      case s: StoreNode => store(s.id)
      case t: ToolNode => tool(t.id)
    }
  }
  
  test("postOrder()") {
    import Helpers._

    doTraversalTest(Trees.abcd, _.postOrder, visited => {
      assert(visited.take(4).toSet == Set(tool(C), store(C), tool(D), store(D)))

      assert(visited.drop(4) == Seq(tool(B), store(B), tool(A)))
      
      assert(visited.size == 7)
    })
  }

  test("iterator()") {
    import Helpers._
    
    doTraversalTest(Trees.abcd, _.iterator, visited => {
      assert(visited.take(4).toSet == Set(tool(C), store(C), tool(D), store(D)))

      assert(visited.drop(4) == Seq(tool(B), store(B), tool(A)))
      
      assert(visited.size == 7)
    })
  }

  test("preOrder()") {
    import Helpers._
    
    doTraversalTest(Trees.abcd, _.preOrder, visited => {
      assert(visited.drop(3).toSet == Set(tool(C), store(C), tool(D), store(D)))

      assert(visited.take(3) == Seq(tool(A), store(B), tool(B)))
      
      assert(visited.size == 7)
    })
  }

  test(s"1 ToolNode depends on 1 StoreNode") {
    val ast = a.gets(Q).from(b(B))

    val expected = ToolNode(a.tool, Map(Q -> b.output(B)))

    assert(ast == expected)
  }

  test("output(LId) and apply(LId)") {
    intercept[Exception] {
      a.output(Z)
    }
    
    intercept[Exception] {
      a(Z)
    }
    
    val expected = StoreNode(a.tool.outputs(A), Set(a))
    
    assert(a.output(A) == expected)

    assert(a(A) == expected)
  }
  
  test("output() on ToolNode with 1 output") {
    val expected = StoreNode(a.tool.outputs(A), Set(a))
    
    assert(a.output() == expected)

    assert(a(A) == expected)
  }
  
  test("output() on ToolNode with 0 outputs") {
    val toolWithNoOutputs = SimpleTool(ToolSpec(Map.empty, Map.empty), Z)
    
    val noOutputs = a.copy(tool = toolWithNoOutputs)
    
    intercept[Exception] {
      noOutputs.output()
    }
  }
  
  test("output() on ToolNode with 2+ outputs") {
    val spec = a.tool.outputs.values.head.spec
    
    val toolWith2Outputs = SimpleTool(ToolSpec(Map.empty, Map(X -> spec, Y -> spec)), Z)
    
    val twoOutputs = a.copy(tool = toolWith2Outputs)
    
    intercept[Exception] {
      twoOutputs.output()
    }
  }
  
  test("producedBy on a StoreNode") {
    val storeSpec = a.tool.outputs.values.head.spec
    
    val store = SimpleStore(storeSpec, A)
    
    val storeNode = StoreNode(store)
    
    val expected = StoreNode(store, Set(a))
    
    assert(storeNode.producedBy(a) == expected)
    
    val expected2 = StoreNode(store, Set(a, b))
    
    assert(storeNode.producedBy(a).producedBy(b) == expected2)
  }
  
  test("remove() on leaves does nothing") {
    assert(a.remove(a) eq a)
    assert(a.remove(b) eq a)
    
    assert(b.remove(b) eq b)
    assert(b.remove(a) eq b)
    
    assert(b.remove(Trees.abcd) eq b)
    assert(a.remove(Trees.abcd) eq a)
  }
  
  test("remove()ing self does nothing") {
    assert(a.remove(a) eq a)
    assert(b.remove(b) eq b)
    assert(Trees.abcd.remove(Trees.abcd) eq Trees.abcd)
    assert(Trees.bcd.remove(Trees.bcd) eq Trees.bcd)
  }
  
  /*
   * //a -> b -> (c, d)
   * 
   * c
   *  \
   *   C
   *    \
   *     b - A - a
   *    /
   *   D 
   *  /
   * b
   */
  test("remove()") {
    //lazy val bcd = b.gets(I).from(c(C)).gets(J).from(d(D))

    //lazy val abcd = a.gets(I).from(bcd(B))
    
    {
      val expected = {
        ToolNode(b.tool, Map(I -> StoreNode(c.tool.outputs(C), Set(c)),
                             J -> StoreNode(d.tool.outputs(D))))
      }
      
      assert(Trees.bcd.remove(d) == expected)
    }
    
    {
      val expected = {
        ToolNode(b.tool, Map(I -> StoreNode(c.tool.outputs(C)),
                             J -> StoreNode(d.tool.outputs(D), Set(d))))
      }
      
      assert(Trees.bcd.remove(c) == expected)
    }
    
    {
      val expected = {
        ToolNode(b.tool, Map(I -> StoreNode(c.tool.outputs(C)),
                             J -> StoreNode(d.tool.outputs(D))))
      }
      
      assert(Trees.bcd.remove(c).remove(d) == expected)
    }
    
    {
      val expected = {
        ToolNode(b.tool, Map(J -> StoreNode(d.tool.outputs(D))))
      }
      
      val cStore = StoreNode(c.tool.outputs(C))
      
      assert(Trees.bcd.remove(c).remove(d).remove(cStore) == expected)
    }
    
    {
      val expected = {
        ToolNode(b.tool)
      }
      
      val cStore = StoreNode(c.tool.outputs(C))
      
      val dStore = StoreNode(d.tool.outputs(D))
      
      assert(Trees.bcd.remove(c).remove(d).remove(cStore).remove(dStore) == expected)
    }
    
    {
      val expected = {
        ToolNode(a.tool, Map(I -> StoreNode(b.tool.outputs(B), Set(
            ToolNode(b.tool, Map(
                I -> StoreNode(c.tool.outputs(C), Set(c)),
                J -> StoreNode(d.tool.outputs(D))))))))
      }
      
      assert(Trees.abcd.remove(d) == expected)
    }
    
    {
      val expected = {
        ToolNode(a.tool, Map(I -> StoreNode(b.tool.outputs(B))))
      }
      
      val cStore = StoreNode(c.tool.outputs(C))
      
      val dStore = StoreNode(d.tool.outputs(D))
      
      assert(Trees.abcd.remove(c).remove(d).remove(cStore).remove(dStore).remove(b) == expected)
    }
  }
  
  /*
   * 2
   *  \
   *   2+2 - (2+2)+1
   *  /
   * 2
   */
  test("removeAll()") {
    {
      val expected = {
        ToolNode(a.tool, Map(I -> StoreNode(b.tool.outputs(B))))
      }
      
      val cStore = StoreNode(c.tool.outputs(C))
      
      val dStore = StoreNode(d.tool.outputs(D))
      
      assert(Trees.abcd.remove(c).remove(d).remove(cStore).remove(dStore).remove(b) == expected)
    }
    
    assert(a.removeAll(Set(a, b, c)) eq a)
    
    assert(b.removeAll(Set(a, b, c)) eq b)
    
    assert(a.removeAll(Set(a, b, Trees.abcd)) eq a)
    assert(b.removeAll(Set(a, b, Trees.abcd)) eq b)
    
    {
      val expected = {
        ToolNode(a.tool, Map(I -> StoreNode(b.tool.outputs(B))))
      }
      
      val cStore = StoreNode(c.tool.outputs(C))
      
      val dStore = StoreNode(d.tool.outputs(D))
      
      assert(Trees.abcd.removeAll(Seq(c, d, cStore, dStore, b)) == expected)
    }
    
    {
      val expected = a
      
      val cStore = StoreNode(c.tool.outputs(C))
      
      val dStore = StoreNode(d.tool.outputs(D))
      
      val bStore = StoreNode(b.tool.outputs(B))
      
      assert(Trees.abcd.removeAll(Seq(c, d, cStore, dStore, b, bStore)) == expected)
    }
    
    {
      val expected = a
      
      val cStore = StoreNode(c.tool.outputs(C))
      
      val dStore = StoreNode(d.tool.outputs(D))
      
      val bStore = StoreNode(b.tool.outputs(B))
      
      assert(Trees.abcd.removeAll(Seq(c, d, cStore, dStore, b, bStore, a, a, a)) == expected)
    }
  }
  
  test("'Complex' pipeline") {

    /*
     *            b
     *          /   \
     *  a <- x <- c  <- y
     *          \   /
     *            d
     */
    val b2y = b.gets(I).from(y(E))
    val c2y = c.gets(I).from(y(F))
    val d2y = d.gets(I).from(y(G))

    val x2bcd = x.gets(B).from(b2y(B)).gets(C).from(c2y(C)).gets(D).from(d2y(D))

    val a2y = a.gets(I).from(x2bcd(X))

    val expected = {
      val storeSpec = StoreSpec(typeOf[Map[Int, Double]])
      
      val xStore = SimpleStore(storeSpec, X)
      
      val xToolNode = ToolNode(x.tool, Map(
          B -> StoreNode(b.tool.outputs(B), Set(b.gets(I).from(y(E)))),
          C -> StoreNode(c.tool.outputs(C), Set(c.gets(I).from(y(F)))),
          D -> StoreNode(d.tool.outputs(D), Set(d.gets(I).from(y(G))))))
      
      ToolNode(a.tool, Map(I -> StoreNode(xStore, Set(xToolNode))))
    }

    assert(a2y == expected)

    /*
     *            b
     *          /   \
     *  a <- x <- c  <- y
     *          \   /
     *            d
     */

    import Helpers._ 
    
    val expectedChunks = Seq(
      Set(tool(Y)),
      Set(store(E), store(F), store(G)),
      Set(tool(B), tool(C), tool(D)),
      Set(store(B), store(C), store(D)),
      Set(tool(X)),
      Set(store(X)),
      Set(tool(A)))  
    
    assert(a2y.chunks.map(_.map(dagNodeToString)).toIndexedSeq == expectedChunks)
  }
  
  test("chunks") {
    import Helpers._
    
    {
      val expected = Seq(
        Set(tool(C), tool(D)),
        Set(store(C), store(D)),
        Set(tool(B)))
        
      assert(Trees.bcd.chunks.map(_.map(dagNodeToString)) == expected)
    }
    
    {
      val expected = Seq(
        Set(tool(C), tool(D)),
        Set(store(C), store(D)),
        Set(tool(B)),
        Set(store(B)),
        Set(tool(A)))
        
      assert(Trees.abcd.chunks.map(_.map(dagNodeToString)) == expected)
    }
  }

  test("toString") {
    //NB: Assert that we can get *something* out of an AST's .toString();
    //previously, when AST mixed in Iterable[AST], .toString would fail with
    //a StackOverflowError.
    assert(Trees.abcd.toString != "")
  }

  private object Trees {
    //a -> b -> (c, d)

    lazy val bcd = b.gets(I).from(c(C)).gets(J).from(d(D))

    lazy val abcd = a.gets(I).from(bcd(B))
    
    println("ABCD: ")
    
    abcd.print()
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

    final case class SimpleStore(spec: StoreSpec, id: LId) extends Store

    final case class SimpleTool(spec: ToolSpec, id: LId) extends Tool {
      private def toStoreMap(m: Map[LId, StoreSpec]): Map[LId, Store] = m.map { case (id, spec) => id -> SimpleStore(spec, id) }

      override val inputs: Map[LId, Store] = toStoreMap(spec.inputs)

      override val outputs: Map[LId, Store] = toStoreMap(spec.outputs)
    }

    //NB: These specs are all totally bogus, and are basically placeholders just to have a way to make unique nodes.
    //That's fine for now since we don't 'typecheck' ASTs.  This will change in the near future.

    private val hStoreSpec = StoreSpec(typeOf[Map[Int, Double]])
    private val zStoreSpec = hStoreSpec
    private val storeSpec = hStoreSpec

    val zSpec = ToolSpec(inputs = Map(H -> hStoreSpec), outputs = Map(Z -> zStoreSpec))

    val hSpec = ToolSpec(inputs = Map(T -> storeSpec, U -> storeSpec, V -> storeSpec), outputs = Map(H -> hStoreSpec))

    val tSpec = ToolSpec(inputs = Map(E -> storeSpec), outputs = Map(T -> storeSpec))
    val uSpec = ToolSpec(inputs = Map(F -> storeSpec), outputs = Map(U -> storeSpec))
    val vSpec = ToolSpec(inputs = Map(G -> storeSpec), outputs = Map(V -> storeSpec))

    val eSpec = ToolSpec(inputs = Map(Y -> storeSpec), outputs = Map(E -> storeSpec))
    val fSpec = ToolSpec(inputs = Map(Y -> storeSpec), outputs = Map(F -> storeSpec))
    val gSpec = ToolSpec(inputs = Map(Y -> storeSpec), outputs = Map(G -> storeSpec))

    val xSpec = ToolSpec(inputs = Map(A -> storeSpec), outputs = Map(X -> storeSpec))
    val ySpec = ToolSpec(inputs = Map(), outputs = Map(E -> storeSpec, F -> storeSpec, G -> storeSpec))

    val aSpec = ToolSpec(inputs = Map(), outputs = Map(A -> storeSpec))

    val bSpec = ToolSpec(inputs = Map(I -> storeSpec), outputs = Map(B -> storeSpec))
    val cSpec = ToolSpec(inputs = Map(I -> storeSpec), outputs = Map(C -> storeSpec))
    val dSpec = ToolSpec(inputs = Map(I -> storeSpec), outputs = Map(D -> storeSpec))
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
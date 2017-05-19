package loamstream.model

import loamstream.TestHelpers.config
import loamstream.compiler.LoamPredef.store
import loamstream.loam.LoamCmdTool.StringContextWithCmd
import loamstream.loam.ops.StoreType.TXT
import loamstream.loam.{LoamProjectContext, LoamScriptContext, LoamTool}
import loamstream.util.TypeBox
import org.scalatest.FunSuite


/**
  * @author clint
  *         date: May 2, 2016
  */
final class AstTest extends FunSuite {

  // scalastyle:off magic.number
  import AST._
  import Ids._
  import Nodes._
  import Tools._

  private implicit val projectContext = LoamProjectContext.empty(config)
  private implicit val scriptContext = new LoamScriptContext(projectContext)

  private val stores = ((1 to 7) ++ (24 to 25)).map(key => (key, store[TXT])).toMap
  //  prontln("aStore is " + aStore.id)

  private def toolEntry(key: Int, inputs: Set[Int], outputs: Set[Int]): (Int, LoamTool) =
    (key, cmd"$key".in(inputs.map(stores)).out(outputs.map(stores)))

  private val tools = Set(
    toolEntry(1, Set(), Set(1)),
    toolEntry(2, Set(24), Set(2)),
    toolEntry(3, Set(24), Set(3)),
    toolEntry(4, Set(24), Set(4)),
    toolEntry(5, Set(25), Set(5)),
    toolEntry(6, Set(25), Set(6)),
    toolEntry(7, Set(25), Set(7))
  ).toMap

  //  prontln("aTool is " + aTool.id)

  test("leaves()") {
    assert(nodes(1).leaves == Set(nodes(1)))

    assert(nodes(1).dependsOn(nodes(2).apply(stores(2).id).as(X)).leaves == Set(nodes(2)))

    assert(
      nodes(1).dependsOn(nodes(2).apply(stores(2).id).as(X)).dependsOn(nodes(3).apply(stores(3).id).as(Y))
        .leaves
      == Set(nodes(2), nodes(3)))

    //a -> b -> (c, d)

    assert(Trees.abcd.leaves == Set(nodes(3), nodes(4)))
  }

  test("isLeaf") {
    assert(nodes(1).isLeaf === true)

    assert(nodes(1).dependsOn(nodes(2).apply(stores(2).id).as(X)).isLeaf === false)

    assert(Trees.abcd.isLeaf === false)
    assert(Trees.bcd.isLeaf === false)
    assert(nodes(3).isLeaf === true)
    assert(nodes(4).isLeaf === true)
  }

  private def doTraversalTest(ast: AST, iteratorFrom: AST => Iterator[AST], validate: Seq[LId] => Unit): Unit = {

    def idsFrom(asts: Iterator[AST]): Seq[LId] = {
      asts.map(_.id).toIndexedSeq
    }

    {
      //just node a

      val visited = idsFrom(iteratorFrom(nodes(1)))

      assert(visited == Seq(tools(1).id))
    }

    val visited = idsFrom(iteratorFrom(ast))

    validate(visited)
  }

  test("postOrder()") {
    doTraversalTest(Trees.abcd, _.postOrder, visited => {
      assert(visited.take(2).toSet == Set(tools(3).id, tools(4).id))

      assert(visited.drop(2) == Seq(tools(2).id, tools(1).id))
    })
  }

  test("iterator()") {
    doTraversalTest(Trees.abcd, _.iterator, visited => {
      assert(visited.take(2).toSet == Set(tools(3).id, tools(4).id))

      assert(visited.drop(2) == Seq(tools(2).id, tools(1).id))
    })
  }

  test("preOrder()") {
    doTraversalTest(Trees.abcd, _.preOrder, visited => {
      assert(visited.take(2) == Seq(tools(1).id, tools(2).id))

      assert(visited.drop(2).toSet == Set(tools(3).id, tools(4).id))
    })
  }

  test(s"1 node dependsOn 1 other node (${classOf[NamedOutput].getSimpleName}) => AST") {
    val ast = nodes(1) dependsOn (nodes(2).apply(Z) as Q)

    val expected = ToolNode(tools(1).id, nodes(1).tool, Set(Connection(Q, Z, nodes(2))))

    assert(ast == expected)
  }

  test("1 node dependsOn 1 other node (id, ast) => AST") {
    val ast = nodes(1).dependsOn(Q, Z, nodes(2))

    val expected = ToolNode(tools(1).id, nodes(1).tool, Set(Connection(Q, Z, nodes(2))))

    assert(ast == expected)
  }

  test("1 node dependsOn 1 other node (connection) => AST") {
    val connection = Connection(stores(1).id, stores(2).id, nodes(2))

    val ast = nodes(1).dependsOn(connection)

    val expected = ToolNode(tools(1).id, nodes(1).tool, Set(connection))

    assert(ast == expected)
  }

  test("1 node dependsOn 1 other node get(id).from(named dep)") {
    val ast = nodes(1).get(Z).from(nodes(2).apply(X))

    val expected = ToolNode(tools(1).id, nodes(1).tool, Set(Connection(Z, X, nodes(2))))

    assert(ast == expected)
  }

  test("1 node dependsOn 1 other node get(iid).from(oid).from(producer)") {
    val ast = nodes(1).get(Z).from(X).from(nodes(2))

    val expected = ToolNode(tools(1).id, nodes(1).tool, Set(Connection(Z, X, nodes(2))))

    assert(ast == expected)
  }

  test("output(LId) and apply(LId)") {
    assert(nodes(1).output(Z) == NamedOutput(Z, nodes(1)))

    assert(nodes(1)(Z) == NamedOutput(Z, nodes(1)))
  }

  test("'Complex' pipeline") {
    /*
     *            b
     *          /   \
     *  a <- x <- c  <- y
     *          \   /
     *            d
     */

    val b2y = nodes(2).dependsOn(yNode(Y).as(I))
    val c2y = nodes(3).dependsOn(yNode(Y).as(I))
    val d2y = nodes(4).dependsOn(yNode(Y).as(I))

    val bcd =
      Set(b2y.output(stores(2).id).as(stores(2).id),
        c2y.output(stores(3).id).as(stores(3).id), d2y.output(stores(4).id).as(stores(4).id))

    val x2bcd = xNode.withDependencies(bcd)

    val a2y = nodes(1).dependsOn(x2bcd(X).as(I))

    val expected = {
      nodes(1).dependsOn(I, X, xNode.withDependencies {
        Set(
          nodes(2).dependsOn(yNode(Y).as(I)).output(stores(2).id).as(stores(2).id),
          nodes(3).dependsOn(yNode(Y).as(I)).output(stores(3).id).as(stores(3).id),
          nodes(4).dependsOn(yNode(Y).as(I)).output(stores(4).id).as(stores(4).id))
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
    assert(getChildOf(a2y, X, stores(2).id).dependencies.size == 1)
    assert(getChildOf(a2y, X, stores(3).id).dependencies.size == 1)
    assert(getChildOf(a2y, X, stores(4).id).dependencies.size == 1)

    assert(getChildOf(a2y, X, stores(2).id, Y).dependencies.size === 0)
    assert(getChildOf(a2y, X, stores(3).id, Y).dependencies.size === 0)
    assert(getChildOf(a2y, X, stores(4).id, Y).dependencies.size === 0)

    val visitCounts: Map[LId, Int] = {
      a2y.iterator.map(_.id).toIndexedSeq.groupBy(identity).mapValues(_.size)
    }

    val expectedCounts = Map(
      tools(1).id -> 1,
      X -> 1,
      tools(2).id -> 1,
      tools(3).id -> 1,
      tools(4).id -> 1,
      Y -> 3)

    assert(visitCounts == expectedCounts)
  }

  test("toString") {
    //NB: Assert that we can get *something* out of an AST's .toString();
    //previously, when AST mixed in Iterable[AST], .toString would fail with
    //a StackOverflowError.
    assert(Trees.abcd.toString != "")
  }

  private object Trees {
    //a -> b -> (c, d)

    lazy val bcd: AST =
      nodes(2).dependsOn(nodes(3).apply(stores(3).id).as(I)).dependsOn(nodes(4).apply(stores(4).id).as(J))

    lazy val abcd: AST = nodes(1).get(stores(2).id).from(I).from(bcd)
  }

  private object Nodes {
    val nodes = (1 to 7).map(key => (key, ToolNode(tools(key)))).toMap

    val hNode = ToolNode(SimpleTool(hSpec, H))
    val tNode = ToolNode(SimpleTool(tSpec, T))
    val uNode = ToolNode(SimpleTool(uSpec, U))
    val vNode = ToolNode(SimpleTool(vSpec, V))
    val xNode = ToolNode(SimpleTool(xSpec, X))
    val yNode = ToolNode(SimpleTool(ySpec, Y))
    val zNode = ToolNode(SimpleTool(zSpec, Z))
  }

  private object Tools {

    final case class SimpleStore(sig: TypeBox.Untyped, id: LId = LId.newAnonId) extends Store

    final case class SimpleTool(spec: ToolSpec, id: LId = LId.newAnonId) extends Tool {
      private def toStoreMap(m: Map[LId, TypeBox.Untyped]): Map[LId, Store] = m.mapValues(SimpleStore(_))

      override val inputs: Map[LId, Store] = toStoreMap(spec.inputs)

      override val outputs: Map[LId, Store] = toStoreMap(spec.outputs)
    }

    //NB: These specs are all totally bogus, and are basically placeholders just to have a way to make unique nodes.
    //That's fine for now since we don't 'typecheck' ASTs.  This will change in the near future.

    private val hStoreSpec = TypeBox.of[Map[Int, Double]]
    private val zStoreSpec = hStoreSpec
    private val storeSpec = hStoreSpec

    val zSpec = ToolSpec(inputs = Map(H -> hStoreSpec), outputs = Map(Z -> zStoreSpec))

    val hSpec = ToolSpec(inputs = Map(T -> storeSpec, U -> storeSpec, V -> storeSpec), outputs = Map(H -> hStoreSpec))

    val tSpec = ToolSpec(inputs = Map(E -> storeSpec), outputs = Map(T -> storeSpec))
    val uSpec = ToolSpec(inputs = Map(F -> storeSpec), outputs = Map(U -> storeSpec))
    val vSpec = ToolSpec(inputs = Map(G -> storeSpec), outputs = Map(V -> storeSpec))

    val xSpec = ToolSpec(inputs = Map(stores(1).id -> storeSpec), outputs = Map(X -> storeSpec))
    val ySpec = ToolSpec(inputs = Map(), outputs = Map(E -> storeSpec, F -> storeSpec, G -> storeSpec))
  }

  private object Ids {
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
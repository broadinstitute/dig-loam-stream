package loamstream.model

import loamstream.TestHelpers.config
import loamstream.compiler.LoamPredef.store
import loamstream.loam.LoamCmdTool.StringContextWithCmd
import loamstream.loam.ops.StoreType.TXT
import loamstream.loam._
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
  import Stores._
  import Tools._

  private implicit val projectContext = LoamProjectContext.empty(config)
  private implicit val scriptContext = new LoamScriptContext(projectContext)

  test("leaves()") {
    assert(nodeA.leaves == Set(nodeA))

    assert(nodeA.dependsOn(nodeB.apply(storeB.id).as(X)).leaves == Set(nodeB))

    assert(
      nodeA.dependsOn(nodeB.apply(storeB.id).as(X)).dependsOn(nodeC.apply(storeC.id).as(Y)).leaves == Set(nodeB, nodeC)
    )

    //a -> b -> (c, d)

    assert(Trees.abcd.leaves == Set(nodeC, nodeD))
  }

  test("isLeaf") {
    assert(nodeA.isLeaf === true)

    assert(nodeA.dependsOn(nodeA.apply(storeB.id).as(X)).isLeaf === false)

    assert(Trees.abcd.isLeaf === false)
    assert(Trees.bcd.isLeaf === false)
    assert(nodeC.isLeaf === true)
    assert(nodeD.isLeaf === true)
  }

  private def doTraversalTest(ast: AST, iteratorFrom: AST => Iterator[AST], validate: Seq[LId] => Unit): Unit = {

    def idsFrom(asts: Iterator[AST]): Seq[LId] = {
      asts.map(_.id).toIndexedSeq
    }

    {
      //just node a

      val visited = idsFrom(iteratorFrom(nodeA))

      assert(visited == Seq(toolA.id))
    }

    val visited = idsFrom(iteratorFrom(ast))

    validate(visited)
  }

  test("postOrder()") {
    doTraversalTest(Trees.abcd, _.postOrder, visited => {
      assert(visited.take(2).toSet == Set(toolC.id, toolD.id))

      assert(visited.drop(2) == Seq(toolB.id, toolA.id))
    })
  }

  test("iterator()") {
    doTraversalTest(Trees.abcd, _.iterator, visited => {
      assert(visited.take(2).toSet == Set(toolC.id, toolD.id))

      assert(visited.drop(2) == Seq(toolB.id, toolA.id))
    })
  }

  test("preOrder()") {
    doTraversalTest(Trees.abcd, _.preOrder, visited => {
      assert(visited.take(2) == Seq(toolA.id, toolB.id))

      assert(visited.drop(2).toSet == Set(toolC.id, toolD.id))
    })
  }

  test(s"1 node dependsOn 1 other node (${classOf[NamedOutput].getSimpleName}) => AST") {
    val ast = nodeA dependsOn (nodeB.apply(Z) as Q)

    val expected = ToolNode(toolA.id, nodeA.tool, Set(Connection(Q, Z, nodeB)))

    assert(ast == expected)
  }

  test("1 node dependsOn 1 other node (id, ast) => AST") {
    val ast = nodeA.dependsOn(Q, Z, nodeB)

    val expected = ToolNode(toolA.id, nodeA.tool, Set(Connection(Q, Z, nodeB)))

    assert(ast == expected)
  }

  test("1 node dependsOn 1 other node (connection) => AST") {
    val connection = Connection(storeA.id, storeB.id, nodeB)

    val ast = nodeA.dependsOn(connection)

    val expected = ToolNode(toolA.id, nodeA.tool, Set(connection))

    assert(ast == expected)
  }

  test("1 node dependsOn 1 other node get(id).from(named dep)") {
    val ast = nodeA.get(Z).from(nodeB.apply(X))

    val expected = ToolNode(toolA.id, nodeA.tool, Set(Connection(Z, X, nodeB)))

    assert(ast == expected)
  }

  test("1 node dependsOn 1 other node get(iid).from(oid).from(producer)") {
    val ast = nodeA.get(Z).from(X).from(nodeB)

    val expected = ToolNode(toolA.id, nodeA.tool, Set(Connection(Z, X, nodeB)))

    assert(ast == expected)
  }

  test("output(LId) and apply(LId)") {
    assert(nodeA.output(Z) == NamedOutput(Z, nodeA))

    assert(nodeA(Z) == NamedOutput(Z, nodeA))
  }

  test("'Complex' pipeline") {
    /*
     *            b
     *          /   \
     *  a <- x <- c  <- y
     *          \   /
     *            d
     */

    val b2y = nodeB.dependsOn(yNode(Y).as(I))
    val c2y = nodeC.dependsOn(yNode(Y).as(I))
    val d2y = nodeD.dependsOn(yNode(Y).as(I))

    val bcd =
      Set(b2y.output(storeB.id).as(storeB.id),
        c2y.output(storeC.id).as(storeC.id), d2y.output(storeD.id).as(storeD.id))

    val x2bcd = xNode.withDependencies(bcd)

    val a2y = nodeA.dependsOn(x2bcd(X).as(I))

    val expected = {
      nodeA.dependsOn(I, X, xNode.withDependencies {
        Set(
          nodeB.dependsOn(yNode(Y).as(I)).output(storeB.id).as(storeB.id),
          nodeC.dependsOn(yNode(Y).as(I)).output(storeC.id).as(storeC.id),
          nodeD.dependsOn(yNode(Y).as(I)).output(storeD.id).as(storeD.id))
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
    assert(getChildOf(a2y, X, storeB.id).dependencies.size == 1)
    assert(getChildOf(a2y, X, storeC.id).dependencies.size == 1)
    assert(getChildOf(a2y, X, storeD.id).dependencies.size == 1)

    assert(getChildOf(a2y, X, storeB.id, Y).dependencies.size === 0)
    assert(getChildOf(a2y, X, storeC.id, Y).dependencies.size === 0)
    assert(getChildOf(a2y, X, storeD.id, Y).dependencies.size === 0)

    val visitCounts: Map[LId, Int] = {
      a2y.iterator.map(_.id).toIndexedSeq.groupBy(identity).mapValues(_.size)
    }

    val expectedCounts = Map(
      toolA.id -> 1,
      X -> 1,
      toolB.id -> 1,
      toolC.id -> 1,
      toolD.id -> 1,
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
      nodeB.dependsOn(nodeC.apply(storeC.id).as(I)).dependsOn(nodeD.apply(storeD.id).as(J))

    lazy val abcd: AST = nodeA.get(storeB.id).from(I).from(bcd)
  }

  private object Nodes {
    val nodeA: ToolNode = ToolNode(toolA)
    val nodeB: ToolNode = ToolNode(toolB)
    val nodeC: ToolNode = ToolNode(toolC)
    val nodeD: ToolNode = ToolNode(toolD)
    val nodeE: ToolNode = ToolNode(toolE)
    val nodeF: ToolNode = ToolNode(toolF)
    val nodeG: ToolNode = ToolNode(toolG)

    val xNode = ToolNode(SimpleTool(xSpec, X))
    val yNode = ToolNode(SimpleTool(ySpec, Y))
  }

  private object Stores {
    val storeA: LoamStore[TXT] = store[TXT]
    val storeB: LoamStore[TXT] = store[TXT]
    val storeC: LoamStore[TXT] = store[TXT]
    val storeD: LoamStore[TXT] = store[TXT]
    val storeE: LoamStore[TXT] = store[TXT]
    val storeF: LoamStore[TXT] = store[TXT]
    val storeG: LoamStore[TXT] = store[TXT]
    val storeH: LoamStore[TXT] = store[TXT]
    val storeI: LoamStore[TXT] = store[TXT]
    val storeJ: LoamStore[TXT] = store[TXT]
    val storeK: LoamStore[TXT] = store[TXT]
    val storeL: LoamStore[TXT] = store[TXT]
    val storeM: LoamStore[TXT] = store[TXT]
    val storeN: LoamStore[TXT] = store[TXT]
    val storeO: LoamStore[TXT] = store[TXT]
    val storeP: LoamStore[TXT] = store[TXT]
    val storeQ: LoamStore[TXT] = store[TXT]
    val storeR: LoamStore[TXT] = store[TXT]
    val storeS: LoamStore[TXT] = store[TXT]
    val storeT: LoamStore[TXT] = store[TXT]
    val storeU: LoamStore[TXT] = store[TXT]
    val storeV: LoamStore[TXT] = store[TXT]
    val storeW: LoamStore[TXT] = store[TXT]
    val storeX: LoamStore[TXT] = store[TXT]
    val storeY: LoamStore[TXT] = store[TXT]
    val storeZ: LoamStore[TXT] = store[TXT]
  }

  private object Tools {

    val toolA: LoamCmdTool = cmd"a".out(storeA)
    val toolB: LoamCmdTool = cmd"b".in(storeX).out(storeB)
    val toolC: LoamCmdTool = cmd"c".in(storeX).out(storeC)
    val toolD: LoamCmdTool = cmd"d".in(storeX).out(storeD)
    val toolE: LoamCmdTool = cmd"e".in(storeY).out(storeE)
    val toolF: LoamCmdTool = cmd"f".in(storeY).out(storeF)
    val toolG: LoamCmdTool = cmd"g".in(storeY).out(storeG)

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

    val xSpec = ToolSpec(inputs = Map(storeA.id -> storeSpec), outputs = Map(X -> storeSpec))
    val ySpec = ToolSpec(inputs = Map(), outputs = Map(E -> storeSpec, F -> storeSpec, G -> storeSpec))
  }

  private object Ids {
    val E = LId.LNamedId("E")
    val F = LId.LNamedId("F")
    val G = LId.LNamedId("G")
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
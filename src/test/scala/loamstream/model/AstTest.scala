package loamstream.model

import loamstream.TestHelpers.config
import loamstream.compiler.LoamPredef.store
import loamstream.loam.LoamCmdTool.StringContextWithCmd
import loamstream.loam._
import loamstream.loam.ops.StoreType.TXT
import org.scalatest.FunSuite


/**
  * @author clint
  *         date: May 2, 2016
  */
final class AstTest extends FunSuite {

  // scalastyle:off magic.number
  import AST._
  import Nodes._
  import Stores._
  import Tools._

  private implicit val projectContext = LoamProjectContext.empty(config)
  private implicit val scriptContext = new LoamScriptContext(projectContext)

  test("leaves()") {
    assert(nodeA.leaves == Set(nodeA))

    assert(nodeA.dependsOn(nodeB(storeB.id).as(storeX.id)).leaves == Set(nodeB))

    assert(
      nodeA.dependsOn(nodeB(storeB.id).as(storeX.id)).dependsOn(nodeC(storeC.id).as(storeY.id)).leaves
        == Set(nodeB, nodeC)
    )

    //a -> b -> (c, d)

    assert(Trees.abcd.leaves == Set(nodeC, nodeD))
  }

  test("isLeaf") {
    assert(nodeA.isLeaf === true)

    assert(nodeA.dependsOn(nodeA(storeB.id).as(storeX.id)).isLeaf === false)

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
    val ast = nodeA dependsOn nodeB(storeZ.id).as(storeQ.id)

    val expected = ToolNode(toolA.id, nodeA.tool, Set(Connection(storeQ.id, storeZ.id, nodeB)))

    assert(ast == expected)
  }

  test("1 node dependsOn 1 other node (id, ast) => AST") {
    val ast = nodeA.dependsOn(storeQ.id, storeZ.id, nodeB)

    val expected = ToolNode(toolA.id, nodeA.tool, Set(Connection(storeQ.id, storeZ.id, nodeB)))

    assert(ast == expected)
  }

  test("1 node dependsOn 1 other node (connection) => AST") {
    val connection = Connection(storeA.id, storeB.id, nodeB)

    val ast = nodeA.dependsOn(connection)

    val expected = ToolNode(toolA.id, nodeA.tool, Set(connection))

    assert(ast == expected)
  }

  test("1 node dependsOn 1 other node get(id).from(named dep)") {
    val ast = nodeA.get(storeZ.id).from(nodeB(storeX.id))

    val expected = ToolNode(toolA.id, nodeA.tool, Set(Connection(storeZ.id, storeX.id, nodeB)))

    assert(ast == expected)
  }

  test("1 node dependsOn 1 other node get(iid).from(oid).from(producer)") {
    val ast = nodeA.get(storeZ.id).from(storeX.id).from(nodeB)

    val expected = ToolNode(toolA.id, nodeA.tool, Set(Connection(storeZ.id, storeX.id, nodeB)))

    assert(ast == expected)
  }

  test("output(LId) and apply(LId)") {
    assert(nodeA.output(storeZ.id) == NamedOutput(storeZ.id, nodeA))

    assert(nodeA(storeZ.id) == NamedOutput(storeZ.id, nodeA))
  }

  test("'Complex' pipeline") {
    /*
     *            b
     *          /   \
     *  a <- x <- c  <- y
     *          \   /
     *            d
     */

    val b2y = nodeB.dependsOn(nodeY(storeY.id).as(storeI.id))
    val c2y = nodeC.dependsOn(nodeY(storeY.id).as(storeI.id))
    val d2y = nodeD.dependsOn(nodeY(storeY.id).as(storeI.id))

    val bcd =
      Set(b2y.output(storeB.id).as(storeB.id),
        c2y.output(storeC.id).as(storeC.id), d2y.output(storeD.id).as(storeD.id))

    val x2bcd = nodeX.withDependencies(bcd)

    val a2y = nodeA.dependsOn(x2bcd(storeX.id).as(storeI.id))

    val expected = {
      nodeA.dependsOn(storeI.id, storeX.id, nodeX.withDependencies {
        Set(
          nodeB.dependsOn(nodeY(storeY.id).as(storeI.id)).output(storeB.id).as(storeB.id),
          nodeC.dependsOn(nodeY(storeY.id).as(storeI.id)).output(storeC.id).as(storeC.id),
          nodeD.dependsOn(nodeY(storeY.id).as(storeI.id)).output(storeD.id).as(storeD.id))
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

    assert(getChildOf(a2y, storeX.id).dependencies.size == 3)
    assert(getChildOf(a2y, storeX.id, storeB.id).dependencies.size == 1)
    assert(getChildOf(a2y, storeX.id, storeC.id).dependencies.size == 1)
    assert(getChildOf(a2y, storeX.id, storeD.id).dependencies.size == 1)

    assert(getChildOf(a2y, storeX.id, storeB.id, storeY.id).dependencies.size === 0)
    assert(getChildOf(a2y, storeX.id, storeC.id, storeY.id).dependencies.size === 0)
    assert(getChildOf(a2y, storeX.id, storeD.id, storeY.id).dependencies.size === 0)

    val visitCounts: Map[LId, Int] = {
      a2y.iterator.map(_.id).toIndexedSeq.groupBy(identity).mapValues(_.size)
    }

    val expectedCounts = Map(
      toolA.id -> 1,
      toolX.id -> 1,
      toolB.id -> 1,
      toolC.id -> 1,
      toolD.id -> 1,
      toolY.id -> 3)

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
      nodeB.dependsOn(nodeC(storeC.id).as(storeI.id)).dependsOn(nodeD(storeD.id).as(storeJ.id))

    lazy val abcd: AST = nodeA.get(storeB.id).from(storeI.id).from(bcd)
  }

  private object Nodes {
    val nodeA: ToolNode = ToolNode(toolA)
    val nodeB: ToolNode = ToolNode(toolB)
    val nodeC: ToolNode = ToolNode(toolC)
    val nodeD: ToolNode = ToolNode(toolD)
    val nodeX: ToolNode = ToolNode(toolX)
    val nodeY: ToolNode = ToolNode(toolY)
  }

  private object Stores {
    val storeA: LoamStore[TXT] = store[TXT]
    val storeB: LoamStore[TXT] = store[TXT]
    val storeC: LoamStore[TXT] = store[TXT]
    val storeD: LoamStore[TXT] = store[TXT]
    val storeE: LoamStore[TXT] = store[TXT]
    val storeF: LoamStore[TXT] = store[TXT]
    val storeG: LoamStore[TXT] = store[TXT]
    val storeI: LoamStore[TXT] = store[TXT]
    val storeJ: LoamStore[TXT] = store[TXT]
    val storeQ: LoamStore[TXT] = store[TXT]
    val storeX: LoamStore[TXT] = store[TXT]
    val storeY: LoamStore[TXT] = store[TXT]
    val storeZ: LoamStore[TXT] = store[TXT]
  }

  private object Tools {

    val toolA: LoamCmdTool = cmd"a".out(storeA)
    val toolB: LoamCmdTool = cmd"b".in(storeX).out(storeB)
    val toolC: LoamCmdTool = cmd"c".in(storeX).out(storeC)
    val toolD: LoamCmdTool = cmd"d".in(storeX).out(storeD)
    val toolX: LoamCmdTool = cmd"x".in(storeA).out(storeX)
    val toolY: LoamCmdTool = cmd"y".out(storeE, storeF, storeG)
  }

}
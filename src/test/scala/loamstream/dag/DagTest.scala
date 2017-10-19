package loamstream.dag

import loamstream.dag.DagTest.ExpectedResults
import org.scalatest.FunSuite

/**
  * LoamStream
  * Created by oruebenacker on 13.10.17.
  */
abstract class DagTest[D <: Dag] extends FunSuite {

  case class DagAndExpectations(name: String, dag: D, expectation: ExpectedResults)

  val dagsAndExpectations: Seq[DagAndExpectations]

  def forAllDags(fun: (String, D, ExpectedResults) => Unit) =
    for (dagAndExpectation <- dagsAndExpectations) {
      val name = dagAndExpectation.name
      val dag = dagAndExpectation.dag
      val expectedResults = dagAndExpectation.expectation
      fun(name, dag, expectedResults)
    }

  test("nodes") {
    forAllDags { (name: String, dag: D, expectedResults: ExpectedResults) =>
      assert(dag.nodes.size === expectedResults.nNodes, name)
    }
  }

  test("nextBefore, nextAfter") {
    forAllDags { (name: String, dag: D, _: ExpectedResults) =>
      for (nodeAfter <- dag.nodes) {
        for (nodeBefore <- dag.nextUp(nodeAfter)) {
          assert(dag.nextDown(nodeBefore)(nodeAfter), name)
        }
      }
      for (nodeBefore <- dag.nodes) {
        for (nodeAfter <- dag.nextDown(nodeBefore)) {
          assert(dag.nextUp(nodeAfter)(nodeBefore), name)
        }
      }
    }
  }

  test("nodesAbove, nodesBelow") {
    forAllDags { (name: String, dag: D, _: ExpectedResults) =>
      for (nodeBelow <- dag.nodes) {
        for (nodeAbove <- dag.nodesAbove(nodeBelow)) {
          assert(dag.nodesBelow(nodeAbove)(nodeBelow), name)
        }
      }
      for (nodeAbove <- dag.nodes) {
        for (nodeBelow <- dag.nodesBelow(nodeAbove)) {
          assert(dag.nodesAbove(nodeBelow)(nodeAbove), name)
        }
      }
    }
  }

  test("topNodes, bottomNodes") {
    forAllDags { (name: String, dag: D, expectedResults: ExpectedResults) =>
      assert(dag.topNodes.size === expectedResults.nTopNodes, name)
      assert(dag.bottomNodes.size === expectedResults.nBottomNodes, name)
    }
  }

  test("levelsFromTop, levelsFromBottom") {
    forAllDags { (name: String, dag: D, expectedResults: ExpectedResults) =>
      assert(dag.levelsFromTop.map(_.size) === expectedResults.sizesOfLevelsFromTop, "from top, " + name)
      assert(dag.levelsFromBottom.map(_.size) === expectedResults.sizesOfLevelsFromBottom, "from bottom, " + name)
    }
  }

  private def regroupNodes[N <: D#Node](nodesToLevels: Map[N, Int]): Seq[Set[N]] ={
    val groupedMap = nodesToLevels.groupBy(_._2).mapValues(_.keySet).view.force
    (0 until groupedMap.size).map(groupedMap)
  }

  test("nodesToLevelsFromTop, nodesToLevelsFromBottom") {
    forAllDags { (name: String, dag: D, _: ExpectedResults) =>
      assert(dag.levelsFromTop === regroupNodes(dag.nodesToLevelsFromTop), "from top, " + name)
      assert(dag.levelsFromBottom === regroupNodes(dag.nodesToLevelsFromBottom), "from bottom, " + name)
    }
  }
}

object DagTest {

  case class ExpectedResults(nNodes: Int, nTopNodes: Int, nBottomNodes: Int,
                             sizesOfLevelsFromTop: Seq[Int], sizesOfLevelsFromBottom: Seq[Int])

}

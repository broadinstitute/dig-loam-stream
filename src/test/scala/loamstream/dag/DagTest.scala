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
}

object DagTest {

  case class ExpectedResults(nNodes: Int, nTopNodes: Int, nBottomNodes: Int)

}

package loamstream.dag

import org.scalatest.FunSuite

/**
  * LoamStream
  * Created by oruebenacker on 13.10.17.
  */
abstract class DagTest[D <: Dag] extends FunSuite {

  val dag: D
  val nNodes: Int

  test("nodes"){
    assert(dag.nodes.size === nNodes)
  }

  test("nextBefore, nextAfter") {
    for(nodeAfter <- dag.nodes) {
      for(nodeBefore <- dag.nextUp(nodeAfter)) {
        assert(dag.nextDown(nodeBefore)(nodeAfter))
      }
    }
    for(nodeBefore <- dag.nodes) {
      for(nodeAfter <- dag.nextDown(nodeBefore)) {
        assert(dag.nextUp(nodeAfter)(nodeBefore))
      }
    }
  }

}

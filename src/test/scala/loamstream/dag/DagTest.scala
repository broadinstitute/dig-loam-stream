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
      for(nodeBefore <- dag.nextBefore(nodeAfter)) {
        assert(dag.nextAfter(nodeBefore)(nodeAfter))
      }
    }
    for(nodeBefore <- dag.nodes) {
      for(nodeAfter <- dag.nextAfter(nodeBefore)) {
        assert(dag.nextBefore(nodeAfter)(nodeBefore))
      }
    }
  }

}

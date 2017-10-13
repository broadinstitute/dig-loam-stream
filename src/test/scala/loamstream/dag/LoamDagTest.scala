package loamstream.dag

import loamstream.LoamGraphExamples

/**
  * LoamStream
  * Created by oruebenacker on 13.10.17.
  */
class LoamDagTest extends DagTest[LoamDag] {
  override val dag = LoamDag(LoamGraphExamples.simple)
  override val nNodes = 6
}

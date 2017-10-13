package loamstream.dag

import cats.data.Validated.Valid
import loamstream.LoamGraphExamples
import loamstream.cwl.LoamToWom
import wom.callable.WorkflowDefinition
import wom.graph.Graph

/**
  * LoamStream
  * Created by oruebenacker on 13.10.17.
  */
class WomDagTest extends DagTest[WomDag] {
  val workflow: WorkflowDefinition =
    LoamToWom.loamToWom("simple", LoamGraphExamples.simple).asInstanceOf[Valid[WorkflowDefinition]].a
  val wom: Graph = workflow.graph.asInstanceOf[Valid[Graph]].a
  override val dag = WomDag(wom)
  override val nNodes = 4
}

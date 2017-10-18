package loamstream.dag

import cats.data.Validated.Valid
import loamstream.LoamGraphExamples
import loamstream.cwl.LoamToWom
import loamstream.dag.DagTest.ExpectedResults
import loamstream.loam.LoamGraph
import wom.callable.WorkflowDefinition
import wom.graph.Graph

/**
  * LoamStream
  * Created by oruebenacker on 13.10.17.
  */
class WomDagTest extends DagTest[WomDag] {
  import WomDagTest.toWomDag

  // scalastyle:off magic.number
  override val dagsAndExpectations = Seq(
    DagAndExpectations("simple", toWomDag("simple", LoamGraphExamples.simple),
      ExpectedResults(
        nNodes = 4,
        nTopNodes = 2,
        nBottomNodes = 1
      )
    ),
    DagAndExpectations("slightly more complex",
      toWomDag("slightly more complex", LoamGraphExamples.slightlyMoreComplex),
      ExpectedResults(
        nNodes = 21,
        nTopNodes = 6,
        nBottomNodes = 2
      )
    )
  )
  // scalastyle:on magic.number
}

object WomDagTest {
  def toWomDag(name: String, loam: LoamGraph): WomDag = {
    val workflowDefinition = LoamToWom.loamToWom(name, loam).asInstanceOf[Valid[WorkflowDefinition]].a
    val wom = workflowDefinition.graph.asInstanceOf[Valid[Graph]].a
    WomDag(wom)
  }
}
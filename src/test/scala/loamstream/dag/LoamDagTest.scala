package loamstream.dag

import loamstream.LoamGraphExamples
import loamstream.dag.DagTest.ExpectedResults

/**
  * LoamStream
  * Created by oruebenacker on 13.10.17.
  */
class LoamDagTest extends DagTest[LoamDag] {
  // scalastyle:off magic.number
  override val dagsAndExpectations = Seq(
    DagAndExpectations("simple", LoamDag(LoamGraphExamples.simple),
      ExpectedResults(
        nNodes = 6,
        nTopNodes = 2,
        nBottomNodes = 1,
        sizesOfLevelsFromTop = Seq(2, 1, 1, 1, 1),
        sizesOfLevelsFromBottom = Seq(1, 1, 2, 1, 1)
      )
    ),
    DagAndExpectations("slightly more complex", LoamDag(LoamGraphExamples.slightlyMoreComplex),
      ExpectedResults(
        nNodes = 36,
        nTopNodes = 6,
        nBottomNodes = 2,
        sizesOfLevelsFromTop = Seq(6, 3, 3, 3, 3, 1, 1, 1, 1, 1, 1, 1, 1, 3, 3, 2, 2),
        sizesOfLevelsFromBottom = Seq(2, 2, 3, 3, 1, 1, 1, 1, 2, 2, 4, 3, 4, 2, 3, 1, 1)
      )
    )
  )
  // scalastyle:on magic.number
}

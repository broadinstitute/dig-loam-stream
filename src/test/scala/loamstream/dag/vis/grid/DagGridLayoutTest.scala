package loamstream.dag.vis.grid

import loamstream.dag.MockDag
import loamstream.dag.vis.grid.DagGridLayoutBuilder.DagBox
import org.scalatest.FunSuite

/**
  * LoamStream
  * Created by oliverr on 11/2/2017.
  */
final class DagGridLayoutTest extends FunSuite {

  test("DagGridLayout.NodeRow") {
    val dag = MockDag.empty
    // scalastyle:off magic.number
    val dagBoxNine = DagBox(dag, 9)
    val dagBoxTen = DagBox(dag, 10)
    val emptyRowNine = dagBoxNine.NodeRowBuilder.empty
    val emptyRowTen = dagBoxTen.NodeRowBuilder.empty
    assert(emptyRowNine.isEmpty)
    assert(emptyRowTen.isEmpty)
    assert(emptyRowNine.isFull === false)
    assert(emptyRowTen.isFull === false)
    assert(emptyRowNine.emptyICols === (0 until 9))
    assert(emptyRowTen.emptyICols === (0 until 10))
    assert(emptyRowNine.iColsFromCenter === Seq(4, 3, 5, 2, 6, 1, 7, 0, 8))
    assert(emptyRowTen.iColsFromCenter === Seq(5, 4, 6, 3, 7, 2, 8, 1, 9, 0))
    assert(emptyRowNine.emptyIColsFromCenter === Seq(4, 3, 5, 2, 6, 1, 7, 0, 8))
    assert(emptyRowTen.emptyIColsFromCenter === Seq(5, 4, 6, 3, 7, 2, 8, 1, 9, 0))
  }

}

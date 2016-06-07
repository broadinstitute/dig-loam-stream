package loamstream.tools

import org.scalatest.FunSuite
import loamstream.model.StoreSpec
import loamstream.model.Store
import loamstream.model.LId
import loamstream.model.ToolSpec
import loamstream.model.Tool
import loamstream.model.AST
import loamstream.Sigs


/**
 * Created on: 1/20/16
 *
 * @author Kaan Yuksel
 */
final class ImputationTest extends FunSuite {
  import AST._
  import Nodes._

  test("nothing yet") {
    gToolNode.print()
  }

  private object Tools {
    final case class SimpleStore(spec: StoreSpec, id: LId = LId.newAnonId) extends Store

    final case class SimpleTool(spec: ToolSpec, id: LId = LId.newAnonId) extends Tool {
      private def toStoreMap(m: Map[LId, StoreSpec]): Map[LId, Store] = m.mapValues(SimpleStore(_))

      override val inputs: Map[LId, Store] = toStoreMap(spec.inputs)

      override val outputs: Map[LId, Store] = toStoreMap(spec.outputs)
    }
  }

  private object Nodes {
    import Tools._

    /*
    Finalized (after AST transformations) imputation pipeline:

    s1 -> i1 --
                \
    s2 -> i2 --  \
    .             \
    .              -> g
    .            /
    .           /
    sN -> iN --

    where
    {s1, s2, ... , sN}: ShapeIt ToolNode's to be executed in parallel
    {i1, i2, ... , iN}: Impute2 ToolNode's to be executed in parallel
    g: ToolNode that gathers Impute2 results for the final result

    Below is an example for N = 2
    */

    val intToDouble = Sigs.map[Int, Double]
    
    // ShapeIt
    val sStoreSpec = StoreSpec(intToDouble)

    val s1Id = LId.LNamedId("S1")
    val s1ToolSpec = ToolSpec(inputs = Map(), outputs = Map(s1Id -> sStoreSpec))
    val s1Tool = SimpleTool(s1ToolSpec, s1Id)
    val s1Ast = AST(s1Tool)

    val s2Id = LId.LNamedId("S2")
    val s2StoreSpec = StoreSpec(intToDouble)
    val s2ToolSpec = ToolSpec(inputs = Map(), outputs = Map(s2Id -> s2StoreSpec))
    val s2Tool = SimpleTool(s2ToolSpec, s2Id)
    val s2Ast = AST(s2Tool)

    // Impute2
    val iStoreSpec = StoreSpec(intToDouble)

    val i1Id = LId.LNamedId("I1")
    val i1ToolSpec = ToolSpec(inputs = Map(), outputs = Map(i1Id -> iStoreSpec))
    val i1Tool = SimpleTool(i1ToolSpec, i1Id)

    val i2Id = LId.LNamedId("I2")
    val i2ToolSpec = ToolSpec(inputs = Map(), outputs = Map(i2Id -> iStoreSpec))
    val i2Tool = SimpleTool(i2ToolSpec, i2Id)

    // Gatherer
    val gId = LId.LNamedId("G")
    val g1Id = LId.LNamedId("G1")
    val g2Id = LId.LNamedId("G2")
    val gStoreSpec = StoreSpec(intToDouble)
    val gToolSpec = ToolSpec(inputs = Map(), outputs = Map(gId -> gStoreSpec))
    val gTool = SimpleTool(gToolSpec, gId)

    // Dependencies
    val i1Ast = ToolNode(i1Id, i1Tool).get(i1Id).from(s1Ast(s1Id))
    
    val i2Ast = ToolNode(i2Id, i2Tool).get(i2Id).from(s2Ast(s2Id))

    val gToolNode = ToolNode(gId, gTool, Set(Connection(i1Id, g1Id, i1Ast), Connection(i2Id, g2Id, i2Ast)))
  }
}


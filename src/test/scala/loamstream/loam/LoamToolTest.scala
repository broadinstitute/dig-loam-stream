package loamstream.loam

import org.scalatest.FunSuite
import loamstream.loam.LoamToken.StringToken
import loamstream.model.LId
import loamstream.model.StoreSig

/**
 * @author clint
 * date: Jul 21, 2016
 */
final class LoamToolTest extends FunSuite {
  import LoamTool._
  
  test("string interpolation (trivial)") {
    implicit val builder = new LoamGraphBuilder
    
    val tool = cmd"foo bar baz"
    
    assert(tool.graph eq builder.graph)
    
    assert(tool.graph.stores == Set.empty)
    assert(tool.graph.storeSinks == Map.empty)
    assert(tool.graph.storeSources == Map.empty)

    assert(tool.graph.toolInputs == Map(tool -> Set.empty))
    assert(tool.graph.toolOutputs == Map(tool -> Set.empty))
    
    assert(tool.graph.tools == Set(tool))
    assert(tool.graph.toolTokens == Map(tool -> Seq(StringToken("foo bar baz"))))
    
    assert(tool.inputs == Map.empty)
    assert(tool.outputs == Map.empty)
    assert(tool.tokens == Seq(StringToken("foo bar baz")))
  }
  
  test("in() and out()") {
    
    implicit val builder = new LoamGraphBuilder
    
    val tool = cmd"foo bar baz"
    
    val inStore0 = LoamStore(LId.newAnonId, StoreSig.create[Int])
    val inStore1 = LoamStore(LId.newAnonId, StoreSig.create[String])
    
    val outStore0 = LoamStore(LId.newAnonId, StoreSig.create[Option[Float]])
    val outStore1 = LoamStore(LId.newAnonId, StoreSig.create[Short])
    
    assert(tool.inputs == Map.empty)
    assert(tool.outputs == Map.empty)
    assert(tool.tokens == Seq(StringToken("foo bar baz")))

    val withInputStores = tool.in(inStore0, inStore1)
    
    assert(withInputStores.inputs == Map(inStore0.id -> inStore0, inStore1.id -> inStore1))
    assert(withInputStores.outputs == Map.empty)
    assert(withInputStores.tokens == Seq(StringToken("foo bar baz")))
    
    val withOutputStoresStores = withInputStores.out(outStore0, outStore1)
    
    assert(withOutputStoresStores.inputs == Map(inStore0.id -> inStore0, inStore1.id -> inStore1))
    assert(withOutputStoresStores.outputs == Map(outStore0.id -> outStore0, outStore1.id -> outStore1))
    assert(withOutputStoresStores.tokens == Seq(StringToken("foo bar baz")))
  }
}
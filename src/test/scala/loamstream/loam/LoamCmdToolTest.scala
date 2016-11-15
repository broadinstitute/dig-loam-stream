package loamstream.loam

import loamstream.loam.LoamToken.StringToken
import loamstream.loam.files.LoamFileManager
import loamstream.model.LId
import loamstream.util.TypeBox
import org.scalatest.FunSuite

/**
  * @author clint
  *         date: Jul 21, 2016
  */
final class LoamCmdToolTest extends FunSuite {

  import LoamCmdTool._

  test("string interpolation (trivial)") {
    implicit val scriptContext = new LoamScriptContext(LoamProjectContext.empty)

    val tool = cmd"foo bar baz"

    assert(tool.graph eq scriptContext.projectContext.graphBox.value)

    assert(tool.graph.stores == Set.empty)
    assert(tool.graph.storeSinks == Map.empty)
    assert(tool.graph.storeSources == Map.empty)

    assert(tool.graph.toolInputs == Map(tool -> Set.empty))
    assert(tool.graph.toolOutputs == Map(tool -> Set.empty))

    assert(tool.graph.tools == Set(tool))

    assert(tool.inputs == Map.empty)
    assert(tool.outputs == Map.empty)
    assert(tool.tokens == Seq(StringToken("foo bar baz")))
  }

  test("in() and out()") {

    implicit val projectContext = LoamProjectContext.empty
    implicit val scriptContext = new LoamScriptContext(projectContext)

    val tool = cmd"foo bar baz"

    val inStore0 = LoamStore(LId.newAnonId, TypeBox.of[Int])
    val inStore1 = LoamStore(LId.newAnonId, TypeBox.of[String])

    val outStore0 = LoamStore(LId.newAnonId, TypeBox.of[Option[Float]])
    val outStore1 = LoamStore(LId.newAnonId, TypeBox.of[Short])

    assert(tool.inputs == Map.empty)
    assert(tool.outputs == Map.empty)
    assert(tool.tokens == Seq(StringToken("foo bar baz")))

    val toolWithInputStores = tool.in(inStore0, inStore1)

    assert(toolWithInputStores.inputs == Map(inStore0.id -> inStore0, inStore1.id -> inStore1))
    assert(toolWithInputStores.outputs == Map.empty)
    assert(toolWithInputStores.tokens == Seq(StringToken("foo bar baz")))

    val toolWithOutputStoresStores = toolWithInputStores.out(outStore0, outStore1)

    assert(toolWithOutputStoresStores.inputs == Map(inStore0.id -> inStore0, inStore1.id -> inStore1))
    assert(toolWithOutputStoresStores.outputs == Map(outStore0.id -> outStore0, outStore1.id -> outStore1))
    assert(toolWithOutputStoresStores.tokens == Seq(StringToken("foo bar baz")))
  }

  test("to(...) and from(...)") {
    implicit val scriptContext = new LoamScriptContext(LoamProjectContext.empty)
    import loamstream.compiler.LoamPredef._
    val inStoreWithPath = store[TXT].from("dir/inStoreWithPath.txt")
    val outStoreWithPath = store[TXT].to("dir/outStoreWithPath.txt")
    val inStoreWithUri = store[TXT].from(uri("xyz://host/dir/inStoreWithUri"))
    val outStoreWithUri = store[TXT].from(uri("xyz://host/dir/outStoreWithUri"))
    val tool = cmd"maker $inStoreWithPath $inStoreWithUri $outStoreWithPath $outStoreWithUri"
    val inPath = inStoreWithPath.path
    val outPath = outStoreWithPath.path
    val inUri = inStoreWithUri.uriOpt.get
    val outUri = outStoreWithUri.uriOpt.get
    val commandLineExpected = s"maker $inPath $inUri $outPath $outUri"
    assert(tool.commandLine === commandLineExpected)
  }
}
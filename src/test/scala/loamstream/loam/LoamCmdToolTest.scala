package loamstream.loam

import loamstream.loam.LoamToken.{StoreToken, StringToken}
import loamstream.loam.ops.StoreType.{BIM, TXT, VCF}
import loamstream.model.LId
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

  def storeMap(stores: Iterable[LoamStore.Untyped]): Map[LId, LoamStore.Untyped] =
    stores.map(store => store.id -> store).toMap

  def tokens(values: AnyRef*): Seq[LoamToken] = values.map {
    case string: String => StringToken(string)
    case store: LoamStore.Untyped => StoreToken(store)
  }

  def assertIOStores(tool: LoamCmdTool, expectedTokens: Seq[LoamToken],
                     inputsBefore: Set[LoamStore.Untyped], outputsBefore: Set[LoamStore.Untyped],
                     inStore0: LoamStore.Untyped, inStore1: LoamStore.Untyped,
                     outStore0: LoamStore.Untyped, outStore1: LoamStore.Untyped): Unit = {
    val inputsMapBefore = storeMap(inputsBefore)
    val outputsMapBefore = storeMap(outputsBefore)
    val inputsMapAfter = storeMap(inputsBefore + inStore0 + inStore1)
    val outputsMapAfter = storeMap(outputsBefore + outStore0 + outStore1)

    assert(tool.inputs == inputsMapBefore)
    assert(tool.outputs == outputsMapBefore)
    assert(tool.tokens == expectedTokens)

    val toolWithInputStores = tool.in(inStore0, inStore1)

    assert(toolWithInputStores.inputs == inputsMapAfter)
    assert(toolWithInputStores.outputs == outputsMapBefore)
    assert(toolWithInputStores.tokens == expectedTokens)

    val toolWithOutputStoresStores = toolWithInputStores.out(outStore0, outStore1)

    assert(toolWithOutputStoresStores.inputs == inputsMapAfter)
    assert(toolWithOutputStoresStores.outputs == outputsMapAfter)
    assert(toolWithOutputStoresStores.tokens == expectedTokens)

  }

  test("in() and out() with no implicit i/o stores") {

    implicit val projectContext = LoamProjectContext.empty
    implicit val scriptContext = new LoamScriptContext(projectContext)

    val tool = cmd"foo bar baz"

    val inStore0 = LoamStore[VCF](LId.newAnonId)
    val inStore1 = LoamStore[TXT](LId.newAnonId)

    val outStore0 = LoamStore[VCF](LId.newAnonId)
    val outStore1 = LoamStore[BIM](LId.newAnonId)

    val expectedTokens = tokens("foo bar baz")
    val inputsBefore = Set.empty[LoamStore.Untyped]
    val outputsBefore = Set.empty[LoamStore.Untyped]
    assertIOStores(tool, expectedTokens, inputsBefore, outputsBefore, inStore0, inStore1, outStore0, outStore1)
  }

  test("in() and out() mixed with implicit i/o stores") {

    implicit val projectContext = LoamProjectContext.empty
    implicit val scriptContext = new LoamScriptContext(projectContext)

    val inStoreImplicit = LoamStore[VCF](LId.newAnonId).from("inputFile.vcf")
    val outStoreImplicit = LoamStore[TXT](LId.newAnonId).to("outputFile.txt")

    val tool = cmd"foo $inStoreImplicit $outStoreImplicit"

    val inStore0 = LoamStore[VCF](LId.newAnonId)
    val inStore1 = LoamStore[TXT](LId.newAnonId)

    val outStore0 = LoamStore[VCF](LId.newAnonId)
    val outStore1 = LoamStore[BIM](LId.newAnonId)

    val expectedTokens = tokens("foo ", inStoreImplicit, " ", outStoreImplicit)
    val inputsBefore = Set[LoamStore.Untyped](inStoreImplicit)
    val outputsBefore = Set[LoamStore.Untyped](outStoreImplicit)
    assertIOStores(tool, expectedTokens, inputsBefore, outputsBefore, inStore0, inStore1, outStore0, outStore1)
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
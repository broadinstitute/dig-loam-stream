package loamstream.loam

import java.nio.file.{Path, Files => JFiles}

import loamstream.compiler.LoamEngine
import loamstream.compiler.LoamPredef._
import loamstream.compiler.messages.ClientMessageHandler
import loamstream.loam.LoamCmdTool.StringContextWithCmd
import loamstream.loam.ops.StoreType.TXT
import loamstream.util.PathEnrichments._
import loamstream.util.Validation
import org.scalatest.FunSuite
import loamstream.TestHelpers

/** Tests of LoamNativeTool */
final class LoamNativeToolTest extends FunSuite {

  private val folder = JFiles.createTempDirectory("loamNativeToolTest")
  private val storePaths = (0 until 5).map(index => folder / s"file$index.txt")

  private def createProjectContext: LoamProjectContext = {
    implicit val projectContext = LoamProjectContext.empty(TestHelpers.config)
    implicit val scriptContext = new LoamScriptContext(projectContext)
    val store0 = store[TXT].at(storePaths(0)).asInput
    val store1 = store[TXT].at(storePaths(1))
    val store2 = store[TXT].at(storePaths(2))
    val store3 = store[TXT].at(storePaths(3))
    val store4 = store[TXT].at(storePaths(4)) // scalastyle:ignore magic.number
    cmd"cp $store0 $store1"
    job(store1, store2) {
      JFiles.copy(store1.path, store2.path)
    }
    cmd"cp $store2 $store3"
    job(store3, store4) {
      JFiles.copy(store3.path, store4.path) // scalastyle:ignore magic.number
    }
    projectContext
  }

  private def validateGraph(graph: LoamGraph): Seq[Validation.IssueBase[LoamGraph]] = {
    LoamGraphValidation.allRules.apply(graph)
  }


  private val fileContentString = "Hello World!"

  private def assertFile(path: Path): Unit = {
    assert(JFiles.exists(path), s"File does not exist: $path")
    assert(new String(JFiles.readAllBytes(path)) === fileContentString)
  }

  test("File copy pipeline with native and cmd tools.") {
    val context = createProjectContext
    val graphValidationIssues = LoamGraphValidation.allRules(context.graph)
    assert(graphValidationIssues.isEmpty,
      s"There were some graph validation issues: ${graphValidationIssues.mkString("\n")}")
    JFiles.write(storePaths(0), fileContentString.getBytes)
    val loamEngine = LoamEngine.default(TestHelpers.config)
    val jobResults = loamEngine.run(context)
    assert(jobResults.size === 4, s"Should have gotten 4 results, but got $jobResults")
    assert(jobResults.values.forall(_.isSuccess), s"Not all job results were successful: $jobResults")
    storePaths.foreach(assertFile)
  }

  private def assertInputsAndOutputs(tool: LoamTool, inStores: Set[LoamStore.Untyped],
                             outStores: Set[LoamStore.Untyped]): Unit = {
    assert(tool.inputs.values.toSet === inStores)
    assert(tool.outputs.values.toSet === outStores)
  }

  test("Loam native tool I/O API") {
    import loamstream.compiler.LoamPredef._
    implicit val projectContext = LoamProjectContext.empty(TestHelpers.config)
    implicit val scriptContext = new LoamScriptContext(projectContext)
    val store0 = store[TXT].at(storePaths(0)).asInput
    val store1 = store[TXT].at(storePaths(1))
    val store2 = store[TXT].at(storePaths(2))
    val store3 = store[TXT].at(storePaths(3))
    val store4 = store[TXT].at(storePaths(4)) // scalastyle:ignore magic.number
    val tool0 = job(store0, store1, store2) {
      "Hello!"
    }
    assertInputsAndOutputs(tool0, Set(store0), Set(store1, store2))
    val tool1 = job(in(store0, store1, store2), out(store3, store4)) {
      Seq("East", "West", "North", "South")
    }
    assertInputsAndOutputs(tool1, Set(store0, store1, store2), Set(store3, store4))
    val tool2 = job(in(store0, store1, store2)) {
      Some(2 + 2)
    }
    assertInputsAndOutputs(tool2, Set(store0, store1, store2), Set.empty)
    val tool3 = job(out(store3, store4)) {
      Set("Frodo", "Sam", "Merry", "Pippin", "Legolas", "Gimli", "Aragon", "Boromir", "Gandalf")
    }
    assertInputsAndOutputs(tool3, Set.empty, Set(store3, store4))
  }

}

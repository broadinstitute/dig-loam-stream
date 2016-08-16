package loamstream.loam

import java.nio.file.{Path, Files => JFiles}

import loamstream.compiler.LoamEngine
import loamstream.compiler.LoamPredef._
import loamstream.compiler.messages.ClientMessageHandler
import loamstream.loam.LoamCmdTool.StringContextWithCmd
import loamstream.util.PathEnrichments._
import loamstream.util.{Validation, ValueBox}
import org.scalatest.FunSuite

/** Tests of LoamNativeTool */
class LoamNativeToolTest extends FunSuite {

  val folder = JFiles.createTempDirectory("loamNativeToolTest")
  val storePaths = (0 until 5).map(index => folder / s"file$index.txt")

  def createGraph: LoamGraph = {
    implicit val context = new LoamContext
    val store0 = store[TXT].from(storePaths(0))
    val store1 = store[TXT].to(storePaths(1))
    val store2 = store[TXT].to(storePaths(2))
    val store3 = store[TXT].to(storePaths(3))
    val store4 = store[TXT].to(storePaths(4)) // scalastyle:ignore magic.number
    cmd"cp $store0 $store1"
    job(store1, store2) {
      JFiles.copy(storePaths(1), storePaths(2))
    }
    cmd"cp $store2 $store3"
    job(store3, store4) {
      JFiles.copy(storePaths(3), storePaths(4)) // scalastyle:ignore magic.number
    }
    context.graphBox.value
  }

  def validateGraph(graph: LoamGraph): Seq[Validation.IssueBase[LoamGraph]] =
    LoamGraphValidation.allRules.apply(graph)


  val fileContentString = "Hello World!"

  def assertFile(path: Path): Unit = {
    assert(JFiles.exists(path), s"File does not exist: $path")
    assert(new String(JFiles.readAllBytes(path)) === fileContentString)
  }

  test("File copy pipeline with native and cmd tools.") {
    val graph = createGraph
    val graphValidationIssues = LoamGraphValidation.allRules(graph)
    assert(graphValidationIssues.isEmpty,
      s"There were some graph validation issues: ${graphValidationIssues.mkString("\n")}")
    JFiles.write(storePaths(0), fileContentString.getBytes)
    val loamEngine = LoamEngine.default(ClientMessageHandler.OutMessageSink.NoOp)
    val jobResults = loamEngine.run(graph)
    assert(jobResults.size === 5, s"Should have gotten 5 results, but got $jobResults")
    assert(jobResults.values.forall(_.nonEmpty), s"Did not get results for all jobs: $jobResults")
    assert(jobResults.values.forall(_.get.isSuccess), s"Not all job results were successful: $jobResults")
    storePaths.foreach(assertFile)
  }

  def assertInputsAndOutputs(tool: LoamTool, inStores: Set[LoamStore], outStores: Set[LoamStore]): Unit = {
    assert(tool.inputs.values.toSet === inStores)
    assert(tool.outputs.values.toSet === outStores)
  }

  test("Loam native tool I/O API") {
    import loamstream.compiler.LoamPredef._
    implicit val context = new LoamContext
    val store0 = store[TXT].from(storePaths(0))
    val store1 = store[TXT].to(storePaths(1))
    val store2 = store[TXT].to(storePaths(2))
    val store3 = store[TXT].to(storePaths(3))
    val store4 = store[TXT].to(storePaths(4)) // scalastyle:ignore magic.number
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

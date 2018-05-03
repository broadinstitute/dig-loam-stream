package loamstream.wdl

import loamstream.loam.{LoamCmdTool, LoamGraph, LoamProjectContext, LoamScriptContext, LoamToolBox}
import org.scalatest.FunSuite

class WdlTest extends FunSuite {

  private def createExecutable: LoamGraph = {
    import loamstream.TestHelpers.config

    implicit val scriptContext: LoamScriptContext = new LoamScriptContext(LoamProjectContext.empty(config))

    import LoamCmdTool._
    import loamstream.compiler.LoamPredef._

    val inPath = path("data.txt")
    val outPath = path("report.txt")

    val input = store.at(inPath).asInput
    val tmp1 = store
    val tmp2 = store
    val output = store.at(outPath)

    cmd"grep color $input > $tmp1"
    cmd"grep colour $input > $tmp2"
    cmd"cat $tmp1 $tmp2 > $output"

    scriptContext.projectContext.graph
  }

  test("WDL") {
    val output = new java.io.ByteArrayOutputStream()
    val graph = createExecutable
    val wdl = new WdlGraph("wfloam", graph)

    // dump the wdl to the stream
    wdl.write(output)

    // show it
    println(output.toString)
  }

}

package loamstream.wdl

import loamstream.loam.{LoamCmdTool, LoamProjectContext, LoamScriptContext, LoamToolBox}
import loamstream.model.execute.Executable
import org.scalatest.FunSuite

class WdlTest extends FunSuite {

  private val toolBox = new LoamToolBox()


  private def createExecutable: Executable = {
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

    val graph = scriptContext.projectContext.graph
    toolBox.createExecutable(graph)
  }

  test("WDL") {
    val executable = createExecutable
    val wdl = LoamToWdl.loamToWdl(executable)
    val wdlString = WdlPrinter.print(wdl)
    println(wdlString)
  }

}

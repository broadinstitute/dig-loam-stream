package loamstream.cwl

import loamstream.TestHelpers
import loamstream.TestHelpers.config
import loamstream.compiler.LoamPredef
import loamstream.compiler.LoamPredef.{path, store}
import loamstream.loam.{LoamCmdTool, LoamGraph, LoamProjectContext, LoamScriptContext}
import loamstream.loam.ops.StoreType.VCF
import org.scalatest.FunSuite

/**
  * LoamStream
  * Created by oliverr on 9/19/2017.
  */
final class LoamToWomTest extends FunSuite {

  private val loamGraph: LoamGraph = {
    import TestHelpers.config

    implicit val scriptContext: LoamScriptContext = new LoamScriptContext(LoamProjectContext.empty(config))

    import LoamPredef._
    import LoamCmdTool._

    val inputFile = path("/user/home/someone/data.vcf")
    val outputFile = path("/user/home/someone/dataImputed.vcf")

    val raw = store[VCF].at(inputFile).asInput
    val phased = store[VCF]
    val template = store[VCF].at(path("/home/myself/template.vcf")).asInput
    val imputed = store[VCF].at(outputFile)

    val phaseTool = cmd"shapeit -in $raw -out $phased"
    val imputeTool = cmd"impute -in $phased -template $template -out $imputed".using("R-3.1")
    scriptContext.projectContext.graph
  }

  test("Loam to WOM") {
    val errorOrWorkflow = LoamToWom.toWom("daGraph", loamGraph)
    assert(errorOrWorkflow.isValid)
  }



}

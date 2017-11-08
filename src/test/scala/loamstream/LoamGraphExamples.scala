package loamstream

import java.nio.file.{Files, Path, Paths}

import loamstream.compiler.{LoamEngine, LoamPredef}
import loamstream.loam.{LoamCmdTool, LoamGraph, LoamProjectContext, LoamScriptContext}

import scala.collection.JavaConverters.iterableAsScalaIterableConverter

/**
  * LoamStream
  * Created by oruebenacker on 13.10.17.
  */
object LoamGraphExamples {

  val simple: LoamGraph = {
    import TestHelpers.config

    implicit val scriptContext: LoamScriptContext = new LoamScriptContext(LoamProjectContext.empty(config))

    import LoamCmdTool._
    import LoamPredef._

    val inputFile = path("/user/home/someone/data.vcf")
    val outputFile = path("/user/home/someone/dataImputed.vcf")

    val raw = store.at(inputFile).asInput
    val phased = store
    val template = store.at(path("/home/myself/template.vcf")).asInput
    val imputed = store.at(outputFile)

    cmd"shapeit -in $raw -out $phased"
    cmd"impute -in $phased -template $template -out $imputed".using("R-3.1")
    scriptContext.projectContext.graph
  }

  // scalastyle:off magic.number
  val slightlyMoreComplex: LoamGraph = {
    import TestHelpers.config

    implicit val scriptContext: LoamScriptContext = new LoamScriptContext(LoamProjectContext.empty(config))

    import LoamPredef._
    import LoamCmdTool._
    val nIns = 6
    val ins = Seq.fill(nIns)(store.asInput)
    val nOuts = 15
    val outs = Seq.fill(nOuts)(store)
    cmd"yo0".in(ins(0)).out(outs(0))
    cmd"yo1".in(ins(1)).out(outs(1))
    cmd"yo2".in(ins(2)).out(outs(2))
    cmd"yo3".in(outs(0), ins(3)).out(outs(3))
    cmd"yo4".in(outs(1), ins(4)).out(outs(4))
    cmd"yo5".in(outs(2), ins(5)).out(outs(5))
    cmd"yo6".in(outs(3)).out(outs(6))
    cmd"yo7".in(outs(4), outs(6)).out(outs(7))
    cmd"yo8".in(outs(5), outs(7)).out(outs(8))
    cmd"yo9".in(outs(6), outs(7), outs(8)).out(outs(9))
    cmd"yo10".in(outs(9)).out(outs(10))
    cmd"yo11".in(outs(9)).out(outs(11))
    cmd"yo12".in(outs(9)).out(outs(12))
    cmd"yo13".in(outs(10), outs(11), outs(12)).out(outs(13))
    cmd"yo14".in(outs(10), outs(11), outs(12)).out(outs(14))
    scriptContext.projectContext.graph
  }
  // scalastyle:on magic.number

  val pipelineDirectory: Path = Paths.get("pipeline", "loam")

  def getAnalysisPipelineFiles: Iterable[Path] =
    Files.newDirectoryStream(pipelineDirectory, "*.loam").asScala.toSeq

  def getAnalysisGraph: LoamGraph = {
    val engine = LoamEngine.default(TestHelpers.config)
    val files: Iterable[Path] = getAnalysisPipelineFiles
    assert(files.nonEmpty, s"Could not find any *.loam files in $pipelineDirectory.")
    val resultsShot = engine.compileFiles(files)
    val result = resultsShot.get
    result.contextOpt.get.graph
  }

}

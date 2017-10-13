package loamstream

import java.nio.file.{Files, Path, Paths}

import loamstream.compiler.{LoamEngine, LoamPredef}
import loamstream.loam.ops.StoreType.VCF
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

    val raw = store[VCF].at(inputFile).asInput
    val phased = store[VCF]
    val template = store[VCF].at(path("/home/myself/template.vcf")).asInput
    val imputed = store[VCF].at(outputFile)

    cmd"shapeit -in $raw -out $phased"
    cmd"impute -in $phased -template $template -out $imputed".using("R-3.1")
    scriptContext.projectContext.graph
  }

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

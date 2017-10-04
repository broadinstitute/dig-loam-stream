package loamstream.cwl

import java.nio.file.{Files, Path, Paths}

import cats.data.Validated.{Invalid, Valid}
import lenthall.validation.ErrorOr.ErrorOr
import loamstream.TestHelpers
import loamstream.compiler.{LoamEngine, LoamPredef}
import loamstream.loam.ops.StoreType.VCF
import loamstream.loam.{LoamCmdTool, LoamGraph, LoamProjectContext, LoamScriptContext}
import org.scalatest.FunSuite

import scala.collection.JavaConverters.iterableAsScalaIterableConverter

/**
  * LoamStream
  * Created by oliverr on 9/19/2017.
  */
final class LoamToWomTest extends FunSuite {

  def errorOrToMessage[A](errorOrA: ErrorOr[A]): String = errorOrA match {
    case Valid(a) => s"Looks like a valid $a"
    case Invalid(messages) => messages.toList.mkString("\n", "\n", "\n")
  }

  private val loamGraph: LoamGraph = {
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

  test("Convert toy Loam to WOM") {
    val errorOrWorkflow = LoamToWom.loamToWom("daGraph", loamGraph)
    assert(errorOrWorkflow.isValid, errorOrToMessage(errorOrWorkflow))
  }

  val pipelineDirectory = Paths.get("pipeline", "loam")

  def getAnalysisPipelineFiles: Iterable[Path] =
    Files.newDirectoryStream(pipelineDirectory, "*.loam").asScala.toSeq

  def getAnalysisGraph: LoamGraph = {
    val engine = LoamEngine.default(TestHelpers.config)
    val files: Iterable[Path] = getAnalysisPipelineFiles
    assert(files.nonEmpty, s"Could not find any *.loam files in $pipelineDirectory.")
    val resultsShot = engine.compileFiles(files)
    if (resultsShot.isMiss) fail("Failed to compile files: " + resultsShot.message)
    val result = resultsShot.get
    result.contextOpt.get.graph
  }

  test("Convert analysis pipeline from Loam to WOM") {
    val graph = getAnalysisGraph
    val errorOrWorkflow = LoamToWom.loamToWom("analysis pipeline", graph)
    assert(errorOrWorkflow.isValid, errorOrToMessage(errorOrWorkflow))
  }


}

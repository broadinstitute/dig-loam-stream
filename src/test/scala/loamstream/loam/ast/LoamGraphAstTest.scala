package loamstream.loam.ast

import loamstream.compiler.ClientMessageHandler.OutMessageSink
import loamstream.compiler.LoamCompiler
import loamstream.loam.LoamGraphValidation
import loamstream.model.AST.ToolNode
import org.scalatest.FunSuite

import scala.concurrent.ExecutionContext.Implicits.global

/**
  * LoamStream
  * Created by oliverr on 6/20/2016.
  */
class LoamGraphAstTest extends FunSuite {
  val compiler = new LoamCompiler(OutMessageSink.NoOp)(global)

  def validate(code: String): Unit = {
    val result = compiler.compile(code)
    assert(result.graphOpt.nonEmpty)
    val graph = result.graphOpt.get
    assert(LoamGraphValidation.allRules(graph).isEmpty)
    val mapping = LoamGraphAstMapper.newMapping(graph)
    assert(mapping.toolsUnmapped.isEmpty)
    assert(graph.finalTools === mapping.rootTools)
    for (tool <- graph.tools) {
      assert(mapping.toolAsts.get(tool).nonEmpty)
      val ast = mapping.toolAsts(tool)
      assert(ast.isInstanceOf[ToolNode])
      val toolNode = ast.asInstanceOf[ToolNode]
      assert(tool === toolNode.tool)
      val producersAsAsts = ast.dependencies.map(_.producer)
      assert(producersAsAsts.forall(_.isInstanceOf[ToolNode]))
      val producersAsToolNodes = producersAsAsts.map(_.asInstanceOf[ToolNode])
      assert(producersAsToolNodes.map(_.tool) === graph.toolsPreceding(tool))
    }
  }

  test("Validate AST for impute pipeline POC example") {
    val code =
      """
        |val inputFile = key[Path]
        |val outputFile = key[Path]
        |val phaseCommand = key[String]
        |val imputeCommand = key[String]
        |
        |inputFile := path("/user/home/someone/data.vcf")
        |outputFile := path("/user/home/someone/dataImputed.vcf")
        |phaseCommand := "shapeit"
        |imputeCommand := "impute2"
        |
        |val raw = store[VCF].from(inputFile)
        |val phased = store[VCF]
        |val template = store[VCF].from(path("/home/myself/template.vcf"))
        |val imputed = store[VCF].to(outputFile)
        |
        |cmd"$phaseCommand -in $raw -out $phased"
        |cmd"$imputeCommand -in $phased -template $template -out $imputed"
        | """.stripMargin
    validate(code)
  }
  test("Validate AST for example with multiple final tools, i.e. roots") {
    val code =
      """
        |val in1 = store[VCF].from(path("/home/myself/in1.vcf"))
        |val in2 = store[VCF].from(path("/home/myself/in2.vcf"))
        |val in3 = store[VCF].from(path("/home/myself/in3.vcf"))
        |val temp1 = store[VCF]
        |val temp2 = store[VCF]
        |val temp3 = store[VCF]
        |val out1 = store[VCF].to(path("/home/myself/out1.vcf"))
        |val out2 = store[VCF].to(path("/home/myself/out2.vcf"))
        |
        |cmd"command1 -in $in1 $in2 -out $temp1"
        |cmd"command2 -in $in2 $in3 -out $temp2"
        |cmd"command3 -in $temp1 $temp2 -out $out1"
        |cmd"command4 -in $temp1 $temp3 -out $out2"
        | """.stripMargin
    validate(code)
  }
  test("Validate AST for diamond example (branch and remerge)") {
    val code =
      """
        |val in = store[VCF].from(path("/home/myself/in.vcf"))
        |val tempA1 = store[VCF]
        |val tempB1 = store[VCF]
        |val tempA2 = store[VCF]
        |val tempB2 = store[VCF]
        |val out = store[VCF].to(path("/home/myself/out.vcf"))
        |
        |cmd"command1 -in $in -out $tempA1"
        |cmd"command2 -in $in -out $tempB1"
        |cmd"command3 -in $tempA1 -out $tempA2"
        |cmd"command3 -in $tempB1 -out $tempB2"
        |cmd"command4 -in $tempA2 $tempB2 -out $out"
        | """.stripMargin
    validate(code)
  }
}

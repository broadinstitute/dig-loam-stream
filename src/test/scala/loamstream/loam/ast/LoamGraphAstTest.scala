package loamstream.loam.ast

import loamstream.compiler.LoamCompiler
import loamstream.loam.{LoamGraphValidation, LoamScript}
import loamstream.model.AST.ToolNode
import org.scalatest.FunSuite
import loamstream.TestHelpers
import loamstream.loam.LoamGraph
import loamstream.compiler.LoamPredef
import loamstream.loam.LoamCmdTool


/**
  * LoamStream
  * Created by oliverr on 6/20/2016.
  */
final class LoamGraphAstTest extends FunSuite {
  private val compiler = new LoamCompiler

  private def validate(graph: LoamGraph): Unit = {
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
    val graph = TestHelpers.makeGraph { implicit context =>
      import LoamPredef._
      import LoamCmdTool._
    
      val inputFile = path("/user/home/someone/data.vcf")
      val outputFile = path("/user/home/someone/dataImputed.vcf")
      val phaseCommand = "shapeit"
      val imputeCommand = "impute2"
      
      val raw = store.at(inputFile).asInput
      val phased = store
      val template = store.at(path("/home/myself/template.vcf")).asInput
      val imputed = store.at(outputFile)
      
      cmd"$phaseCommand -in $raw -out $phased"
      cmd"$imputeCommand -in $phased -template $template -out $imputed"
    }
    
    validate(graph)
  }
  test("Validate AST for example with multiple final tools, i.e. roots") {
    val graph = TestHelpers.makeGraph { implicit context =>
      import LoamPredef._
      import LoamCmdTool._
        
      val in1 = store.at(path("/home/myself/in1.vcf")).asInput
      val in2 = store.at(path("/home/myself/in2.vcf")).asInput
      val in3 = store.at(path("/home/myself/in3.vcf")).asInput
      val temp1 = store
      val temp2 = store
      val temp3 = store
      val out1 = store.at(path("/home/myself/out1.vcf"))
      val out2 = store.at(path("/home/myself/out2.vcf"))
      
      cmd"command1 -in $in1 $in2 -out $temp1"
      cmd"command2 -in $in2 $in3 -out $temp2"
      cmd"command3 -in $temp1 $temp2 -out $out1"
      cmd"command4 -in $temp1 $temp3 -out $out2"
    }
    
    validate(graph)
  }
  test("Validate AST for diamond example (branch and remerge)") {
    val graph = TestHelpers.makeGraph { implicit context =>
      import LoamPredef._
      import LoamCmdTool._
      
      val in = store.at(path("/home/myself/in.vcf")).asInput
      val tempA1 = store
      val tempB1 = store
      val tempA2 = store
      val tempB2 = store
      val out = store.at(path("/home/myself/out.vcf"))
      
      cmd"command1 -in $in -out $tempA1"
      cmd"command2 -in $in -out $tempB1"
      cmd"command3 -in $tempA1 -out $tempA2"
      cmd"command3 -in $tempB1 -out $tempB2"
      cmd"command4 -in $tempA2 $tempB2 -out $out"
    }
    
    validate(graph)
  }
}

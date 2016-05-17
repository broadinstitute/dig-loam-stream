package loamstream.model

import org.scalatest.FunSuite

import loamstream.apps.hail.HailPipeline
import loamstream.apps.minimal.MiniPipeline
import loamstream.pipelines.qc.ancestry.AncestryInferencePipeline
import loamstream.util.Maps
import LId.LNamedId

/**
 * @author clint
 * date: May 2, 2016
 */
final class AstTest extends FunSuite {
  import AST._
  
  import ToolSpec.ParamNames.{ input, output }
  
  test("infer dependencies between tools in the MiniPipeline") {
    val pipeline = MiniPipeline("foo")
    
    val ast = AST.fromPipeline(pipeline).get
    
    
    val expected = {
      AST(pipeline.sampleIdsTool).dependsOn(output).from(AST(pipeline.genotypeCallsTool))
    }
    
    ast.print()
    
    println()
    
    expected.print()
    
    assert(ast == expected)
  }
  
  test("infer dependencies between tools in the Hail pipeline (dependsOn) ") {
    val pipeline = HailPipeline("foo", "bar", "baz")
    
    val ast = AST.fromPipeline(pipeline).get
    
    val expected = {
      AST(pipeline.singletonTool).dependsOn(output).from {
        AST(pipeline.vdsTool).dependsOn(output).from {
          AST(pipeline.genotypeCallsTool)
        }
      }
    }
    
    assert(ast == expected)
  }
  
  test("infer dependencies between tools in the ancestry inference pipeline") {
    val pipeline = AncestryInferencePipeline("foo", "bar")
    
    val ast = AST.fromPipeline(pipeline).get
    
    val expected = {
      AST(pipeline.sampleClusteringTool).dependsOn(output).from {
        AST(pipeline.pcaProjectionTool).dependsOn(output).from {
          AST(pipeline.pcaWeightsTool)
        }.dependsOn(output).from {
          AST(pipeline.genotypesTool)
        }
      }
    }
    
    assert(ast == expected)
  }
  
  test("findTerminalTool") {
    final case class ExplicitPipeline(tools: Set[Tool]) extends LPipeline {
      override def stores: Set[Store] = tools.flatMap(_.outputs.values)
    }
    
    import AST.findTerminalTool
    
    //Should fail, no tools
    assert(findTerminalTool(ExplicitPipeline(Set.empty)).isFailure)
    
    val ancestryPipeline = AncestryInferencePipeline("foo", "bar")
    
    val pipelineWith2Terminals = ExplicitPipeline(
        Set(ancestryPipeline.genotypesTool, ancestryPipeline.pcaWeightsTool))
    
    //Should fail, 2 terminals
    assert(findTerminalTool(pipelineWith2Terminals).isFailure)
    
    val hailPipeline = HailPipeline("foo", "bar", "baz")
    
    //Should work
    assert(findTerminalTool(hailPipeline).get === hailPipeline.singletonTool)
    
    assert(findTerminalTool(ancestryPipeline).get == ancestryPipeline.sampleClusteringTool)
  }
  
  test("astFor") {
    val pipeline = AncestryInferencePipeline("foo", "bar")
    
    import Maps.Implicits._
    
    val byOutput: Map[StoreSpec, Set[Tool]] = pipeline.toolsByOutput.mapKeys(_.spec)
    
    def astFor(tool: Tool) = AST.astFor(byOutput)(tool)
    
    import pipeline._
    
    assert(astFor(genotypesTool) === AST(genotypesTool))
    
    assert(astFor(pcaWeightsTool) === AST(pcaWeightsTool))
    
    val expected1 = AST(pcaProjectionTool).dependsOn(output).from {
      AST(genotypesTool)
    }.dependsOn(output).from {
      AST(pcaWeightsTool)
    }
    
    assert(astFor(pcaProjectionTool) === expected1)
    
    assert(astFor(sampleClusteringTool) === AST(sampleClusteringTool).dependsOn(output).from(expected1))
  }
}
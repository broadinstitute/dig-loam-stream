package loamstream.model

import org.scalatest.FunSuite

import loamstream.apps.hail.HailPipeline
import loamstream.apps.minimal.MiniPipeline
import loamstream.pipelines.qc.ancestry.AncestryInferencePipeline

/**
 * @author clint
 * date: May 2, 2016
 */
final class AstTest extends FunSuite {
  import AST._
  
  test("infer dependencies between tools in the MiniPipeline") {
    val pipeline = MiniPipeline("foo")
    
    val ast = AST.fromPipeline(pipeline).get
    
    val expected = {
      AST(pipeline.sampleIdsTool.output).dependsOn {
        AST(pipeline.genotypeCallsTool.output.spec)
      }
    }
    
    assert(ast == expected)
  }
  
  test("infer dependencies between tools in the Hail pipeline (dependsOn) ") {
    val pipeline = HailPipeline("foo", "bar", "baz")
    
    val ast = AST.fromPipeline(pipeline).get
    
    val expected = {
      AST(pipeline.singletonTool.output).dependsOn {
        AST(pipeline.vdsTool.output).dependsOn {
          AST(pipeline.genotypeCallsTool.output)
        }
      }
    }
    
    assert(ast == expected)
  }
  
  test("infer dependencies between tools in the Hail pipeline (~>) ") {
    val pipeline = HailPipeline("foo", "bar", "baz")
    
    val ast = AST.fromPipeline(pipeline).get
    
    val expected = {
      AST(pipeline.genotypeCallsTool.output) ~>
      AST(pipeline.vdsTool.output) ~>
      AST(pipeline.singletonTool.output) 
    }
    
    assert(ast == expected)
  }
  
  test("infer dependencies between tools in the Hail pipeline (thenRun) ") {
    val pipeline = HailPipeline("foo", "bar", "baz")
    
    val ast = AST.fromPipeline(pipeline).get
    
    val expected = {
      AST(pipeline.genotypeCallsTool.output) thenRun
      AST(pipeline.vdsTool.output) thenRun
      AST(pipeline.singletonTool.output) 
    }
    
    assert(ast == expected)
  }
  
  test("infer dependencies between tools in the ancestry inference pipeline") {
    val pipeline = AncestryInferencePipeline("foo", "bar")
    
    val ast = AST.fromPipeline(pipeline).get
    
    val expected = {
      AST(pipeline.sampleClusteringTool.output).dependsOn {
        AST(pipeline.pcaProjectionTool.output).dependsOn(
          AST(pipeline.pcaWeightsTool.output),
          AST(pipeline.genotypesTool.output)
        )
      }
    }
    
    assert(ast == expected)
  }
  
  test("AST Implicits: iterables of tools, branching pipeline") {
    val pipeline = AncestryInferencePipeline("foo", "bar")
    
    val expected = AST.fromPipeline(pipeline).get
    
    import pipeline._
    import AST.Implicits._
    
    val actual1 = {
      Seq(genotypesTool, pcaWeightsTool) thenRun
      pcaProjectionTool thenRun
      sampleClusteringTool
    }
    
    assert(expected === actual1)
    
    val actual2 = {
      Seq(genotypesTool, pcaWeightsTool) ~>
      pcaProjectionTool ~>
      sampleClusteringTool
    }
    
    assert(expected === actual2)
    
    val actual3 = {
      sampleClusteringTool.toAST.dependsOn {
        pcaProjectionTool.toAST.dependsOn(
          genotypesTool, 
          pcaWeightsTool
        )
      }
    }
    assert(expected === actual3)
  }
  
  test("AST Implicits: iterables of tools, linear pipeline") {
    val pipeline = HailPipeline("foo", "bar", "baz")
    
    val expected = AST.fromPipeline(pipeline).get
    
    import pipeline._
    import AST.Implicits._
    
    val actual1 = {
      Seq(genotypeCallsTool) thenRun
      vdsTool thenRun
      singletonTool
    }
    
    assert(expected === actual1)
    
    val actual2 = {
      Seq(genotypeCallsTool) ~>
      vdsTool ~>
      singletonTool
    }
    
    assert(expected === actual2)
  }
  
  test("AST Implicits: single tools, linear pipeline") {
    val pipeline = HailPipeline("foo", "bar", "baz")
    
    val expected = AST.fromPipeline(pipeline).get
    
    import pipeline._
    import AST.Implicits._
    
    val actual1 = {
      genotypeCallsTool thenRun
      vdsTool thenRun
      singletonTool
    }
    
    assert(expected === actual1)
    
    val actual2 = {
      genotypeCallsTool ~>
      vdsTool ~>
      singletonTool
    }
    
    assert(expected === actual2)
    
    val actual3 = {
      singletonTool.dependsOn {
        vdsTool.dependsOn {
          genotypeCallsTool
        }
      }
    }
    
    assert(expected === actual3)
  }
  
  test("findTerminalTool") {
    final case class ExplicitPipeline(tools: Set[Tool]) extends LPipeline {
      override def stores: Set[Store] = tools.map(_.output)
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
  }
  
  test("astFor") {
    val pipeline = AncestryInferencePipeline("foo", "bar")
    
    val byOutput: Map[StoreSpec, Set[Tool]] = pipeline.tools.groupBy(_.output.spec)
    
    def astFor(tool: Tool) = AST.astFor(byOutput)(tool.output.spec)
    
    import pipeline._
    
    assert(astFor(genotypesTool) === AST(genotypesTool.output.spec, Set.empty[AST]))
    
    assert(astFor(pcaWeightsTool) === AST(pcaWeightsTool.output.spec, Set.empty[AST]))
    
    val expected1 = AST(pcaProjectionTool.output.spec, Set(
        AST(genotypesTool.output.spec, Set.empty[AST]),
        AST(pcaWeightsTool.output.spec, Set.empty[AST])))
    
    assert(astFor(pcaProjectionTool) === expected1)
    
    assert(astFor(sampleClusteringTool) === AST(sampleClusteringTool.output.spec, Set(expected1)))
  }
}
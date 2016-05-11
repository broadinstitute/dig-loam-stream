package loamstream.model

import org.scalatest.FunSuite

import loamstream.apps.hail.HailPipeline
import loamstream.apps.minimal.MiniPipeline
import loamstream.pipelines.qc.ancestry.AncestryInferencePipeline
import loamstream.util.Maps

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
      AST(pipeline.sampleIdsTool).dependsOn {
        AST(pipeline.genotypeCallsTool)
      }
    }
    
    assert(ast == expected)
  }
  
  test("infer dependencies between tools in the Hail pipeline (dependsOn) ") {
    val pipeline = HailPipeline("foo", "bar", "baz")
    
    val ast = AST.fromPipeline(pipeline).get
    
    val expected = {
      AST(pipeline.singletonTool).dependsOn {
        AST(pipeline.vdsTool).dependsOn {
          AST(pipeline.genotypeCallsTool)
        }
      }
    }
    
    assert(ast == expected)
  }
  
  test("infer dependencies between tools in the Hail pipeline (~>) ") {
    val pipeline = HailPipeline("foo", "bar", "baz")
    
    val ast = AST.fromPipeline(pipeline).get
    
    val expected = {
      AST(pipeline.genotypeCallsTool) ~>
      AST(pipeline.vdsTool) ~>
      AST(pipeline.singletonTool) 
    }
    
    assert(ast == expected)
  }
  
  test("infer dependencies between tools in the Hail pipeline (thenRun) ") {
    val pipeline = HailPipeline("foo", "bar", "baz")
    
    val ast = AST.fromPipeline(pipeline).get
    
    val expected = {
      AST(pipeline.genotypeCallsTool) thenRun
      AST(pipeline.vdsTool) thenRun
      AST(pipeline.singletonTool) 
    }
    
    assert(ast == expected)
  }
  
  test("infer dependencies between tools in the ancestry inference pipeline") {
    val pipeline = AncestryInferencePipeline("foo", "bar")
    
    val ast = AST.fromPipeline(pipeline).get
    
    val expected = {
      AST(pipeline.sampleClusteringTool).dependsOn {
        AST(pipeline.pcaProjectionTool).dependsOn(
          AST(pipeline.pcaWeightsTool),
          AST(pipeline.genotypesTool)
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
      override def stores: Set[Store] = tools.flatMap(_.outputs)
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
    
    import Maps.Implicits._
    
    val byOutput: Map[StoreSpec, Set[Tool]] = pipeline.toolsByOutput.mapKeys(_.spec)
    
    def astFor(tool: Tool) = AST.astFor(byOutput)(tool)
    
    import pipeline._
    
    assert(astFor(genotypesTool) === AST(genotypesTool.spec, Set.empty[AST]))
    
    assert(astFor(pcaWeightsTool) === AST(pcaWeightsTool.spec, Set.empty[AST]))
    
    val expected1 = AST(pcaProjectionTool.spec, Set(
        AST(genotypesTool.spec, Set.empty[AST]),
        AST(pcaWeightsTool.spec, Set.empty[AST])))
    
    assert(astFor(pcaProjectionTool) === expected1)
    
    assert(astFor(sampleClusteringTool) === AST(sampleClusteringTool.spec, Set(expected1)))
  }
}
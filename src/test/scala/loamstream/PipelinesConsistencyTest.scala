package loamstream

import java.nio.file.Paths

import org.scalatest.FunSuite

import loamstream.model.LPipeline
import loamstream.pipelines.qc.ancestry.AncestryInferencePipeline
import loamstream.tools.klusta.KlustaKwikKonfig
import loamstream.util.PipelineConsistencyChecker

/**
  * LoamStream
  * Created by oliverr on 3/24/2016.
  */
final class PipelinesConsistencyTest extends FunSuite {
  
  private def check(pipeline: LPipeline): Unit = {
    val problems = PipelineConsistencyChecker.check(pipeline)
  
    assert(problems == Set.empty, problems.map(_.message).mkString(",\n"))
  }

  private val vcfFile = Paths.get("foo.vcf")
  private val sampleIdsFile = Paths.get("samples.txt")
  
  test("Mini pipeline is consistent.") {
    check(MiniPipeline(vcfFile, sampleIdsFile))
  }

  test("Ancestry inference pipeline is consistent.") {
    val pcaWeightsFile = Paths.get("pca.weights")
    
    check(AncestryInferencePipeline(vcfFile, pcaWeightsFile, KlustaKwikKonfig.withTempWorkDir("foo")))
  }
}

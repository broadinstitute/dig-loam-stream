package loamstream

import loamstream.apps.hail.HailPipeline
import loamstream.apps.minimal.MiniPipeline
import loamstream.model.LPipeline
import loamstream.pipelines.qc.ancestry.AncestryInferencePipeline
import loamstream.util.PipelineConsistencyChecker
import org.scalatest.FunSuite
import tools.core.LCoreDefaultStoreIds
import java.nio.file.Paths
import loamstream.tools.klusta.KlustaKwikKonfig

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

  test("Hail pipeline is consistent.") {
    val vdsDir = Paths.get("foo.vds")
    val singletonsFile = Paths.get("singletons.foo")
    
    check(HailPipeline(vcfFile, vdsDir, singletonsFile))
  }

  test("Ancestry inference pipeline is consistent.") {
    val pcaWeightsFile = Paths.get("pca.weights")
    
    check(AncestryInferencePipeline(vcfFile, pcaWeightsFile, KlustaKwikKonfig.withTempWorkDir("foo")))
  }
}

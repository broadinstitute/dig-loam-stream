package loamstream

import loamstream.apps.hail.HailPipeline
import loamstream.apps.minimal.MiniPipeline
import loamstream.model.LPipeline
import loamstream.pipelines.qc.ancestry.AncestryInferencePipeline
import loamstream.util.PipelineConsistencyChecker
import org.scalatest.FunSuite
import tools.core.CoreConfig

/**
  * LoamStream
  * Created by oliverr on 3/24/2016.
  */
class PipelinesConsistencyTest extends FunSuite {
  def check(pipeline: LPipeline): Unit = {
    val problems = PipelineConsistencyChecker.check(pipeline)
    assert(problems.isEmpty, problems.map(_.message).mkString("\n"))
  }

  test("Mini pipeline is consistent.") {
    val config = CoreConfig.InteractiveConfig
    val genotypesId = config.env(CoreConfig.Keys.genotypesId)
    check(MiniPipeline(genotypesId))
  }

  test("Hail pipeline is consistent.") {
    val config = CoreConfig.InteractiveConfig
    val genotypesId = config.env(CoreConfig.Keys.genotypesId)
    val vdsId = config.env(CoreConfig.Keys.vdsId)
    val singletonsId = config.env(CoreConfig.Keys.singletonsId)
    check(HailPipeline(genotypesId, vdsId, singletonsId))
  }

  test("Ancestry inference pipeline is consistent.") {
    val config = CoreConfig.InteractiveConfig
    val genotypesId = config.env(CoreConfig.Keys.genotypesId)
    val pcaWeightsId = config.env(CoreConfig.Keys.pcaWeightsId)
    check(AncestryInferencePipeline(genotypesId, pcaWeightsId))
  }


}

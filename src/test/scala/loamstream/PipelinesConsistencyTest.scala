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
    check(MiniPipeline(config.genotypesId))
  }

  test("Hail pipeline is consistent.") {
    val config = CoreConfig.InteractiveConfig
    check(HailPipeline(config.genotypesId, config.vdsId, config.singletonsId))
  }

  test("Ancestry inference pipeline is consistent.") {
    val config = CoreConfig.InteractiveConfig
    check(AncestryInferencePipeline(config.genotypesId, config.pcaWeightsId))
  }


}

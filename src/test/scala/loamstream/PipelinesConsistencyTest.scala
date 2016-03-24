package loamstream

import loamstream.apps.hail.HailPipeline
import loamstream.apps.minimal.MiniPipeline
import loamstream.model.LPipeline
import loamstream.pipelines.qc.ancestry.AncestryInferencePipeline
import loamstream.util.PipelineConsistencyChecker
import org.scalatest.FunSuite

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
    check(MiniPipeline.pipeline)
  }

  test("Hail pipeline is consistent.") {
    check(HailPipeline.pipeline)
  }

  test("Ancestry inference pipeline is consistent.") {
    check(AncestryInferencePipeline.pipeline)
  }


}

package loamstream.apps

import loamstream.apps.minimal.MiniExecuter
import loamstream.conf.ImputationConfig
import loamstream.model.execute.LExecutable
import loamstream.model.jobs.LJob
import loamstream.model.jobs.LJob.{SimpleSuccess, Result}
import loamstream.tools.ImputationTools
import loamstream.util.Loggable

import scala.concurrent.{Future, ExecutionContext}

/**
  * LoamStream
  * Created by kyuksel on 05/02/2016.
  */
object ImputationApp extends Loggable {
  final case class ShapeItJob(inputs: Set[LJob]) extends LJob {
    override def execute(implicit context: ExecutionContext): Future[Result] = {
      Future {
        val shapeItExecutable = ImputationConfig.shapeItExecutable
        val vcf = ImputationConfig.shapeItVcfFile
        val map = ImputationConfig.shapeItMapFile
        val haps = ImputationConfig.shapeItHapFile
        val samples = ImputationConfig.shapeItSampleFile
        val log = ImputationConfig.shapeItLogFile
        val numThreads = ImputationConfig.shapeItNumThreads

        val command = ImputationTools.getShapeItCmdLine(shapeItExecutable, vcf, map, haps, samples, log, numThreads)
        ImputationTools.execute(command)
        SimpleSuccess(s"Phased VCF $vcf")
      }
    }
  }

  def main(args: Array[String]) {
    trace("Attempting to run ShapeIt...")

    val shapeItJob = ShapeItJob(Set.empty)
    val executable = LExecutable(Set(shapeItJob))
    val result = MiniExecuter.execute(executable)
  }
}

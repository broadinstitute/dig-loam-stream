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
  final case class ShapeItJob(inputs: Set[LJob], configFile: String) extends LJob {
    override def execute(implicit context: ExecutionContext): Future[Result] = {
      Future {
        val config = ImputationConfig(configFile)
        val shapeItExecutable = config.shapeItExecutable
        val vcf = config.shapeItVcfFile
        val map = config.shapeItMapFile
        val haps = config.shapeItHapFile
        val samples = config.shapeItSampleFile
        val log = config.shapeItLogFile
        val numThreads = config.shapeItNumThreads.toInt

        val command = ImputationTools.getShapeItCmdLine(shapeItExecutable, vcf, map, haps, samples, log, numThreads)
        ImputationTools.execute(command)
        SimpleSuccess(s"Phased the VCF $vcf")
      }
    }
  }

  // TODO: Replace with command line interface
  def CheckIfArgsValid(args: Array[String]): Unit = {
    val configHelpText = "Please provide path to config file in the form: --config <path> or -c <path>"

    var isArgsValid = false
    val longConfigOptionName = "--config"
    val shortConfigOptionName = "-c"

    if (args.size != 2) {
      error(configHelpText)
      System.exit(-1)
    }

    args(0) match {
      case `longConfigOptionName` => isArgsValid = true
      case `shortConfigOptionName` => isArgsValid = true
      case _ => error(configHelpText) ; System.exit(-1)
    }
  }

  def main(args: Array[String]) {
    CheckIfArgsValid(args)

    trace("Attempting to run ShapeIt...")

    val configFile = args(1)
    val shapeItJob = ShapeItJob(Set.empty, args(1))
    val executable = LExecutable(Set(shapeItJob))
    val result = MiniExecuter.execute(executable)
  }
}

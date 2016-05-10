package loamstream.apps

import java.nio.file.Paths

import loamstream.apps.minimal.MiniExecuter
import loamstream.conf.ImputationConfig
import loamstream.model.execute.LExecutable
import loamstream.model.jobs.LCommandLineJob
import loamstream.tools.LineCommand
import loamstream.util.Loggable

/**
  * LoamStream
  * Created by kyuksel on 05/02/2016.
  */
object ImputationApp extends Loggable {
  final case class ShapeItCommandLine(tokens: Seq[String]) extends LineCommand.CommandLine {
    def commandLine = tokens.mkString(LineCommand.tokenSep)
  }

  // TODO: Replace with command line interface
  def checkIfArgsValid(args: Array[String]): Unit = {
    val configHelpText = "Please provide path to config file in the form: --config <path> or -c <path>"

    var isArgsValid = false
    val longConfigOptionName = "--config"
    val shortConfigOptionName = "-c"

    if (args.length != 2) {
      error(configHelpText)
      System.exit(-1)
    }

    args(0) match {
      case `longConfigOptionName` => isArgsValid = true
      case `shortConfigOptionName` => isArgsValid = true
      case _ => error(configHelpText) ; System.exit(-1)
    }
  }

  def getShapeItCmdLineTokens(shapeItExecutable: String, vcf: String, map: String, haps: String, samples: String,
                              log: String,
                              numThreads: Int = 1): Seq[String] = {
    Seq(shapeItExecutable, "-V", vcf, "-M", map, "-O", haps, samples, "-L", log, "--thread", numThreads.toString)
  }

  def main(args: Array[String]) {
    checkIfArgsValid(args)

    trace("Attempting to run ShapeIt...")

    val configFile = args(1)

    val config = ImputationConfig(configFile)
    val shapeItExecutable = config.shapeItExecutable
    val shapeItWorkDir = config.shapeItWorkDir
    val vcf = config.shapeItVcfFile
    val map = config.shapeItMapFile
    val haps = config.shapeItHapFile
    val samples = config.shapeItSampleFile
    val log = config.shapeItLogFile
    val numThreads = config.shapeItNumThreads

    val commandLine = ShapeItCommandLine(getShapeItCmdLineTokens(shapeItExecutable, vcf, map, haps,
      samples, log, numThreads.toInt))
    val shapeItJob = LCommandLineJob(commandLine, Paths.get(shapeItWorkDir), Set.empty)

    val executable = LExecutable(Set(shapeItJob))
    val result = MiniExecuter.execute(executable)
  }
}

package loamstream.apps

import java.nio.file.Paths

import loamstream.apps.minimal.MiniExecuter
import loamstream.client.Drmaa
import loamstream.conf.{ImputationConfig, UgerConfig}
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
    val configHelpText = "Please provide path to config file in the form: --config <path> or -c <path> " +
      "and specify whether there is a single or bulk job(s) in the form: --bulk <true/false> or -b <true/false>"

    var isArgsValid = false

    val longConfigOptionName = "--config"
    val shortConfigOptionName = "-c"

    val longBulkOptionName = "--bulk"
    val shortBulkOptionName = "-b"

    if (args.length != 4) {
      error(configHelpText)
      System.exit(-1)
    }

    args(0) match {
      case `longConfigOptionName` => isArgsValid = true
      case `shortConfigOptionName` => isArgsValid = true
      case _ => error(configHelpText); System.exit(-1)
    }

    args(2) match {
      case `longBulkOptionName` => isArgsValid = true
      case `shortBulkOptionName` => isArgsValid = true
      case _ => error(configHelpText); System.exit(-1)
    }
  }

  def getShapeItCmdLineTokens(shapeItExecutable: String, vcf: String, map: String, haps: String, samples: String,
                              log: String,
                              numThreads: Int = 1): Seq[String] = {
    Seq(shapeItExecutable, "-V", vcf, "-M", map, "-O", haps, samples, "-L", log, "--thread", numThreads.toString)
  }

  def runShapeItLocally(args: Array[String]): Unit = {
    checkIfArgsValid(args)

    trace("Attempting to run ShapeIt...")

    val configFile = args(1)

    val config = ImputationConfig(configFile)
    val shapeItExecutable = config.shapeItExecutable.toString
    val shapeItWorkDir = config.shapeItWorkDir
    val vcf = config.shapeItVcfFile.toString
    val map = config.shapeItMapFile.toString
    val haps = config.shapeItHapFile.toString
    val samples = config.shapeItSampleFile.toString
    val log = config.shapeItLogFile.toString
    val numThreads = config.shapeItNumThreads

    val shapeItTokens = getShapeItCmdLineTokens(shapeItExecutable, vcf, map, haps, samples, log, numThreads)
    val commandLine = ShapeItCommandLine(shapeItTokens)
    val shapeItJob = LCommandLineJob(commandLine, shapeItWorkDir, Set.empty)

    val executable = LExecutable(Set(shapeItJob))
    val result = MiniExecuter.execute(executable)
  }

  def runShapeItOnUger(args: Array[String]): Unit = {
    checkIfArgsValid(args)

    trace("Attempting to run ShapeIt...")

    val configFile = args(1)
    val shapeItConfig = ImputationConfig(configFile)
    val shapeItScript = shapeItConfig.shapeItScript.toString
    val ugerConfig = UgerConfig(configFile)
    val ugerLog = ugerConfig.ugerLogFile.toString

    val isBulk = args(3)

    val drmaaClient = new Drmaa
    drmaaClient.runJob(Array(shapeItScript, ugerLog, isBulk))
  }

  def main(args: Array[String]) {
    //runShapeItLocally(args)
    runShapeItOnUger(args)
  }
}

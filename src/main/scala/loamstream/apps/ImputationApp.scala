package loamstream.apps

import java.nio.file.Paths

import loamstream.apps.minimal.MiniExecuter
import loamstream.client.Drmaa
import loamstream.conf.{ImputationConfig, UgerConfig}
import loamstream.model.execute.LExecutable
import loamstream.model.jobs.LCommandLineJob
import loamstream.tools.LineCommand
import loamstream.util.Loggable
import java.nio.file.Path

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

    val longConfigOptionName = "--config"
    val shortConfigOptionName = "-c"

    val longBulkOptionName = "--bulk"
    val shortBulkOptionName = "-b"

    def quitWith(msg: String): Unit = {
      error(configHelpText)
      
      System.exit(-1)
    }
    
    if (args.length != 4) {
      quitWith(configHelpText)
    }

    args(0) match {
      case `longConfigOptionName` | `shortConfigOptionName` => ()
      case _ => quitWith(configHelpText)
    }

    args(2) match {
      case `longBulkOptionName` | `shortBulkOptionName` => ()
      case _ => quitWith(configHelpText)
    }
  }

  def getShapeItCmdLineTokens(
      shapeItExecutable: Path, 
      vcf: Path, 
      map: Path, 
      haps: Path, 
      samples: Path,
      log: Path,
      numThreads: Int = 1): Seq[String] = {
    
    Seq(
      shapeItExecutable, 
      "-V", 
      vcf, 
      "-M", 
      map, 
      "-O", 
      haps, 
      samples, 
      "-L", 
      log, 
      "--thread", 
      numThreads).map(_.toString)
  }

  def runShapeItLocally(args: Array[String]): Unit = {
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

    val shapeItTokens = getShapeItCmdLineTokens(shapeItExecutable, vcf, map, haps, samples, log, numThreads)
    val commandLine = ShapeItCommandLine(shapeItTokens)
    val shapeItJob = LCommandLineJob(commandLine, shapeItWorkDir, Set.empty)

    val executable = LExecutable(Set(shapeItJob))
    val result = MiniExecuter.execute(executable)
    
    println(result)
    
    result.foreach { case (job, result) =>
      println(s"$result: $job")
    }
  }

  def runShapeItOnUger(args: Array[String]): Unit = {
    checkIfArgsValid(args)

    trace("Attempting to run ShapeIt...")

    val configFile = args(1)
    val shapeItConfig = ImputationConfig(configFile)
    val shapeItScript = shapeItConfig.shapeItScript.toString
    val ugerConfig = UgerConfig(configFile).get //TODO: Fragile
    val ugerLog = ugerConfig.ugerLogFile.toString

    val isBulk = args(3).toBoolean

    val drmaaClient = new Drmaa
    
    drmaaClient.runJob(shapeItScript, ugerLog, isBulk)
  }

  def main(args: Array[String]) {
    runShapeItLocally(Array("--config", "src/main/resources/loamstream.conf", "--bulk", "false"))
    //runShapeItLocally(args)
    //runShapeItOnUger(args)
  }
}

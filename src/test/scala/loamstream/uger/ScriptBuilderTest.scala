package loamstream.uger

import java.nio.file.Path

import scala.io.Source

import org.scalatest.FunSuite

import loamstream.conf.ImputationConfig
import loamstream.model.jobs.commandline.CommandLineBuilderJob
import loamstream.tools.LineCommand
import loamstream.util.LoamFileUtils
import loamstream.util.Files
import java.nio.file.Paths

/**
  * Created by kyuksel on 2/29/2016.
  */
final class ScriptBuilderTest extends FunSuite {
  test("A shell script is generated out of a CommandLineJob, and can be used to submit a UGER job") {
    val shapeItJob = Seq.fill(3)(getShapeItCommandLineBuilderJob)
    val ugerScriptToRunShapeIt = ScriptBuilder.buildFrom(shapeItJob)

    val scriptFile = Paths.get("src/test/resources/imputation/shapeItUgerSubmissionScript.sh")

    val expectedScript = Files.readFrom(scriptFile)

    assert(ugerScriptToRunShapeIt == expectedScript)
  }

  private final case class ShapeItCommandLine(tokens: Seq[String]) extends LineCommand.CommandLine {
    def commandLine = tokens.mkString(LineCommand.tokenSep)
  }
  
  private def getShapeItCommandLineBuilderJob: CommandLineBuilderJob = {
    val configFile = "src/test/resources/loamstream-test.conf"
    val config = ImputationConfig.fromFile(configFile).get
    val shapeItExecutable = config.shapeIt.executable
    val shapeItWorkDir = config.shapeIt.workDir
    val vcf = config.shapeIt.vcfFile
    val map = config.shapeIt.mapFile
    val hap = config.shapeIt.hapFile
    val samples = config.shapeIt.sampleFile
    val log = config.shapeIt.logFile
    val numThreads = numberOfCpuCores

    val shapeItTokens = getShapeItCmdLineTokens(shapeItExecutable, vcf, map, hap, samples, log, numThreads)
    val commandLine = ShapeItCommandLine(shapeItTokens)

    CommandLineBuilderJob(commandLine, shapeItWorkDir, Set.empty)
  }

  private def getShapeItCmdLineTokens(
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

  private def numberOfCpuCores: Int = Runtime.getRuntime.availableProcessors
}

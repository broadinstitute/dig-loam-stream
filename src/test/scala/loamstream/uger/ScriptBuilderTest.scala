package loamstream.uger

import java.nio.file.Path

import loamstream.conf.ImputationConfig
import loamstream.model.jobs.commandline.CommandLineStringJob
import org.scalatest.FunSuite

import scala.io.Source

/**
  * Created by kyuksel on 2/29/2016.
  */
final class ScriptBuilderTest extends FunSuite {
  test("A shell script is generated out of a CommandLineStringJob, and can be used to submit a UGER job") {
    val shapeItJob = Seq.fill(3)(getShapeItCommandLineStringJob)
    val ugerScriptToRunShapeIt = ScriptBuilder.buildFrom(shapeItJob)
    //TODO Make sure the following way of getting file path works in Windows
    val scriptFile = getClass.getClassLoader.getResource("imputation/shapeItUgerSubmissionScript.sh").getFile
    //TODO Use LoamFileUtils.enclosed to make sure of proper resource closing
    val expectedScript = Source.fromFile(scriptFile).mkString

    assert(ugerScriptToRunShapeIt == expectedScript)
  }

  private def getShapeItCommandLineStringJob: CommandLineStringJob = {
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

    val shapeItTokens = getShapeItCommandLineTokens(shapeItExecutable, vcf, map, hap, samples, log, numThreads)
    val shapeItCommandLineString = getShapeItCommandLineString(shapeItExecutable, vcf, map, hap, samples, log,
      numThreads)

    CommandLineStringJob(shapeItCommandLineString, shapeItWorkDir)
  }

  private def getShapeItCommandLineTokens(
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

  private def getShapeItCommandLineString(
                                       shapeItExecutable: Path,
                                       vcf: Path,
                                       map: Path,
                                       haps: Path,
                                       samples: Path,
                                       log: Path,
                                       numThreads: Int = 1): String = {

    getShapeItCommandLineTokens(shapeItExecutable, vcf, map, haps, samples, log, numThreads).mkString(" ")
  }

  private def numberOfCpuCores: Int = Runtime.getRuntime.availableProcessors
}

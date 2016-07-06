package loamstream.uger

import java.nio.file.{Path, Paths}

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
    val shapeItExecutable = "/some/shapeit/executable"
    val shapeItWorkDir = "someWorkDir"
    val vcf = "/some/vcf/file"
    val map = "/some/map/file"
    val hap = "/some/haplotype/file"
    val samples = "/some/sample/file"
    val log = "/some/log/file"
    val numThreads = 2

    val shapeItTokens = getShapeItCommandLineTokens(shapeItExecutable, vcf, map, hap, samples, log, numThreads)
    val shapeItCommandLineString = getShapeItCommandLineString(shapeItExecutable, vcf, map, hap, samples, log,
      numThreads)

    CommandLineStringJob(shapeItCommandLineString, Paths.get(shapeItWorkDir))
  }

  private def getShapeItCommandLineTokens(
                               shapeItExecutable: String,
                               vcf: String,
                               map: String,
                               haps: String,
                               samples: String,
                               log: String,
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
                                       shapeItExecutable: String,
                                       vcf: String,
                                       map: String,
                                       haps: String,
                                       samples: String,
                                       log: String,
                                       numThreads: Int = 1): String = {

    getShapeItCommandLineTokens(shapeItExecutable, vcf, map, haps, samples, log, numThreads).mkString(" ")
  }
}

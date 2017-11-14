package loamstream.uger

import java.nio.file.Paths

import org.scalatest.FunSuite

import loamstream.model.execute.Environment
import loamstream.model.jobs.commandline.CommandLineJob
import loamstream.TestHelpers

/**
 * Created by kyuksel on 2/29/2016.
 */
object ScriptBuilderTest {
  private final implicit class EnrichedString(val string: String) extends AnyVal {
    def withNormalizedLineBreaks: String = string.replaceAll("\r\n", "\n")
  }
}

final class ScriptBuilderTest extends FunSuite {

  import ScriptBuilderTest.EnrichedString

  test("A shell script is generated out of a CommandLineJob, and can be used to submit a UGER job") {
    val jobs = Seq(getShapeItCommandLineJob(0), getShapeItCommandLineJob(1), getShapeItCommandLineJob(2))
    val ugerScriptContents = ScriptBuilder.buildFrom(jobs).withNormalizedLineBreaks
    val expectedScriptContents = expectedScriptAsString(0, 1, 2).withNormalizedLineBreaks

    assert(ugerScriptContents == expectedScriptContents)
  }

  import TestHelpers.path
  
  private def getShapeItCommandLineJob(discriminator: Int): CommandLineJob = {
    val shapeItExecutable = "/some/shapeit/executable"
    val shapeItWorkDir = path("someWorkDir")
    val vcf = s"/some/vcf/file.$discriminator"
    val map = s"/some/map/file.$discriminator"
    val hap = s"/some/haplotype/file.$discriminator"
    val samples = "/some/sample/file"
    val log = "/some/log/file"
    val numThreads = 2

    val commandLineString = getShapeItCommandLineString(shapeItExecutable, vcf, map, hap, samples, log, numThreads)

    CommandLineJob(commandLineString, shapeItWorkDir, Environment.Local)
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
  // scalastyle:off method.length
  // scalastyle:off line.size.limit
  private def expectedScriptAsString(discriminator0: Int, discriminator1: Int, discriminator2: Int): String = {
    val sixSpaces = "      "

    s"""#!/bin/bash
#$$ -cwd
#$$ -j y

source /broad/software/scripts/useuse
reuse -q UGER
reuse -q Java-1.8

export PATH=/humgen/diabetes/users/dig/miniconda2/bin:$$PATH
conda env create -f /humgen/diabetes/users/dig/hail/environment.yml
source activate hail

i=$$SGE_TASK_ID
$sixSpaces
if [ $$i -eq 1 ]
then
\t/some/shapeit/executable -V /some/vcf/file.$discriminator0 -M /some/map/file.$discriminator0 -O /some/haplotype/file.$discriminator0 /some/sample/file -L /some/log/file --thread 2
elif [ $$i -eq 2 ]
then
\t/some/shapeit/executable -V /some/vcf/file.$discriminator1 -M /some/map/file.$discriminator1 -O /some/haplotype/file.$discriminator1 /some/sample/file -L /some/log/file --thread 2
elif [ $$i -eq 3 ]
then
\t/some/shapeit/executable -V /some/vcf/file.$discriminator2 -M /some/map/file.$discriminator2 -O /some/haplotype/file.$discriminator2 /some/sample/file -L /some/log/file --thread 2
fi
"""
  }
  // scalastyle:on line.size.limit
  // scalastyle:on method.length
}

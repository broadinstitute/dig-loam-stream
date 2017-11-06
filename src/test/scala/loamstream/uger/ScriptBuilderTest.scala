package loamstream.uger

import java.nio.file.Paths

import org.scalatest.FunSuite

import loamstream.model.execute.Environment
import loamstream.model.jobs.commandline.CommandLineStringJob

/**
  * Created by kyuksel on 2/29/2016.
  */
final class ScriptBuilderTest extends FunSuite {

  private implicit class EnrichedString(string: String) {
    def withNormalizedLineBreaks: String = string.replaceAll("\r\n", "\n")
  }

  test("A shell script is generated out of a CommandLineStringJob, and can be used to submit a UGER job") {
    val shapeItJob = Seq.fill(3)(getShapeItCommandLineStringJob)
    val ugerScriptToRunShapeIt = ScriptBuilder.buildFrom(shapeItJob).withNormalizedLineBreaks
    val expectedScript = expectedScriptAsString.withNormalizedLineBreaks

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

    CommandLineStringJob(shapeItCommandLineString, Paths.get(shapeItWorkDir), Environment.Local)
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
  def expectedScriptAsString: String = {
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
\t/some/shapeit/executable -V /some/vcf/file -M /some/map/file -O /some/haplotype/file /some/sample/file -L /some/log/file --thread 2
elif [ $$i -eq 2 ]
then
\t/some/shapeit/executable -V /some/vcf/file -M /some/map/file -O /some/haplotype/file /some/sample/file -L /some/log/file --thread 2
elif [ $$i -eq 3 ]
then
\t/some/shapeit/executable -V /some/vcf/file -M /some/map/file -O /some/haplotype/file /some/sample/file -L /some/log/file --thread 2
fi
"""
  }
  // scalastyle:on line.size.limit
  // scalastyle:on method.length
}

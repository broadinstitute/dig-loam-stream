package loamstream.drm

import org.scalatest.FunSuite
import loamstream.model.execute.Environment
import loamstream.model.jobs.commandline.CommandLineJob
import loamstream.TestHelpers
import loamstream.conf.ExecutionConfig
import loamstream.util.BashScript.Implicits._
import scala.collection.Seq
import loamstream.uger.UgerScriptBuilderParams
import loamstream.uger.UgerPathBuilder

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

  //TODO: test making LSF scripts!! 
  
  test("A shell script is generated out of a CommandLineJob, and can be used to submit a UGER job") {
    val ugerConfig = TestHelpers.config.ugerConfig.get

    val jobs = Seq(getShapeItCommandLineJob(0), getShapeItCommandLineJob(1), getShapeItCommandLineJob(2))
    val jobName = DrmTaskArray.makeJobName()
    val taskArray = DrmTaskArray.fromCommandLineJobs(ExecutionConfig.default, ugerConfig, UgerPathBuilder, jobs, jobName)
    val ugerScriptContents = (new ScriptBuilder(UgerScriptBuilderParams)).buildFrom(taskArray).withNormalizedLineBreaks

    val jobIds: (String, String, String) = (jobs(0).id.toString, jobs(1).id.toString, jobs(2).id.toString)
    val discriminators = (0, 1, 2)
    
    val expectedScriptContents = expectedScriptAsString(jobName, discriminators, jobIds).withNormalizedLineBreaks

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
  private def expectedScriptAsString(
      jobName: String, 
      discriminators: (Int, Int, Int), 
      jobIds: (String, String, String)): String = {
    
    val (discriminator0, discriminator1, discriminator2) = discriminators
    val (jobId0, jobId1, jobId2) = jobIds

    val ugerDir = path("/humgen/diabetes/users/kyuksel/imputation/shapeit_example").toAbsolutePath.render
    val outputDir = path("out/job-outputs").toAbsolutePath.render

    val sixSpaces = "      "

    // scalastyle:off line.size.limit
    s"""#!/bin/bash
#$$ -cwd

source /broad/software/scripts/useuse
reuse -q UGER
reuse -q Java-1.8

export PATH=/humgen/diabetes/users/dig/miniconda2/bin:$$PATH
source activate loamstream_v1.0

i=$$SGE_TASK_ID
jobId=$$JOB_ID
$sixSpaces
if [ $$i -eq 1 ]
then
/some/shapeit/executable -V /some/vcf/file.$discriminator0 -M /some/map/file.$discriminator0 -O /some/haplotype/file.$discriminator0 /some/sample/file -L /some/log/file --thread 2

LOAMSTREAM_JOB_EXIT_CODE=$$?

stdoutDestPath="$outputDir/${jobId0}.stdout"
stderrDestPath="$outputDir/${jobId0}.stderr"

mkdir -p $outputDir
mv $ugerDir/${jobName}.1.stdout $$stdoutDestPath || echo "Couldn't move Uger std out log" > $$stdoutDestPath
mv $ugerDir/${jobName}.1.stderr $$stderrDestPath || echo "Couldn't move Uger std err log" > $$stderrDestPath

exit $$LOAMSTREAM_JOB_EXIT_CODE

elif [ $$i -eq 2 ]
then
/some/shapeit/executable -V /some/vcf/file.$discriminator1 -M /some/map/file.$discriminator1 -O /some/haplotype/file.$discriminator1 /some/sample/file -L /some/log/file --thread 2

LOAMSTREAM_JOB_EXIT_CODE=$$?

stdoutDestPath="$outputDir/${jobId1}.stdout"
stderrDestPath="$outputDir/${jobId1}.stderr"

mkdir -p $outputDir
mv $ugerDir/${jobName}.2.stdout $$stdoutDestPath || echo "Couldn't move Uger std out log" > $$stdoutDestPath
mv $ugerDir/${jobName}.2.stderr $$stderrDestPath || echo "Couldn't move Uger std err log" > $$stderrDestPath

exit $$LOAMSTREAM_JOB_EXIT_CODE

elif [ $$i -eq 3 ]
then
/some/shapeit/executable -V /some/vcf/file.$discriminator2 -M /some/map/file.$discriminator2 -O /some/haplotype/file.$discriminator2 /some/sample/file -L /some/log/file --thread 2

LOAMSTREAM_JOB_EXIT_CODE=$$?

stdoutDestPath="$outputDir/${jobId2}.stdout"
stderrDestPath="$outputDir/${jobId2}.stderr"

mkdir -p $outputDir
mv $ugerDir/${jobName}.3.stdout $$stdoutDestPath || echo "Couldn't move Uger std out log" > $$stdoutDestPath
mv $ugerDir/${jobName}.3.stderr $$stderrDestPath || echo "Couldn't move Uger std err log" > $$stderrDestPath

exit $$LOAMSTREAM_JOB_EXIT_CODE

fi
"""
  // scalastyle:on line.size.limit
  }
  // scalastyle:on method.length
}

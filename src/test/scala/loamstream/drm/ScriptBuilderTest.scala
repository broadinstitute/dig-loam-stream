package loamstream.drm

import org.scalatest.FunSuite
import loamstream.model.execute.Environment
import loamstream.model.jobs.commandline.CommandLineJob
import loamstream.TestHelpers
import loamstream.conf.ExecutionConfig
import loamstream.util.BashScript.Implicits._
import scala.collection.Seq
import loamstream.drm.uger.UgerScriptBuilderParams
import loamstream.drm.uger.UgerPathBuilder
import loamstream.model.execute.DrmSettings
import loamstream.conf.DrmConfig
import loamstream.model.quantities.Cpus
import loamstream.model.quantities.Memory
import loamstream.model.quantities.CpuTime
import loamstream.drm.lsf.LsfDockerParams
import loamstream.drm.lsf.LsfPathBuilder
import loamstream.drm.lsf.LsfScriptBuilderParams

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
    def doTest(drmSystem: DrmSystem, dockerParamsOpt: Option[DockerParams]): Unit = {
      val drmConfig: DrmConfig = drmSystem match {
        case DrmSystem.Lsf => TestHelpers.configWithLsf.lsfConfig.get
        case DrmSystem.Uger => TestHelpers.configWithUger.ugerConfig.get
      }
      
      val defaultQueue: Option[Queue] = drmSystem match {
        case DrmSystem.Lsf => None
        case DrmSystem.Uger => Some(Queue("broad"))
      }
      
      val pathBuilder: PathBuilder = drmSystem match {
        case DrmSystem.Lsf => LsfPathBuilder
        case DrmSystem.Uger => UgerPathBuilder
      }
      
      val scriptBuilderParams: ScriptBuilderParams = drmSystem match {
        case DrmSystem.Lsf => LsfScriptBuilderParams
        case DrmSystem.Uger => UgerScriptBuilderParams
      }
      
      val drmSettings = drmSystem.settingsMaker(
          Cpus(1),
          Memory.inGb(42),
          CpuTime.inHours(2),
          defaultQueue,
          dockerParamsOpt)
  
      val jobs = Seq(getShapeItCommandLineJob(0), getShapeItCommandLineJob(1), getShapeItCommandLineJob(2))
      val jobName = DrmTaskArray.makeJobName()
      
      val taskArray = DrmTaskArray.fromCommandLineJobs(
          ExecutionConfig.default,
          drmSettings,
          drmConfig, 
          pathBuilder, 
          jobs, 
          jobName)
          
      val scriptContents = (new ScriptBuilder(scriptBuilderParams)).buildFrom(taskArray).withNormalizedLineBreaks
  
      val jobIds: (String, String, String) = (jobs(0).id.toString, jobs(1).id.toString, jobs(2).id.toString)
      val discriminators = (0, 1, 2)
      
      val expectedScriptContents = expectedScriptAsString(
        drmConfig, 
        jobName, 
        discriminators, 
        jobIds, 
        drmSystem, 
        dockerParamsOpt).withNormalizedLineBreaks
  
      assert(scriptContents == expectedScriptContents)
    }
    
    val dockerParams = LsfDockerParams("library/foo:1.23")
    
    doTest(DrmSystem.Uger, None)
    doTest(DrmSystem.Lsf, None)
    doTest(DrmSystem.Lsf, Some(dockerParams))
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
      drmConfig: DrmConfig,
      jobName: String, 
      discriminators: (Int, Int, Int), 
      jobIds: (String, String, String),
      drmSystem: DrmSystem, 
      dockerParamsOpt: Option[DockerParams]): String = {
    
    val (discriminator0, discriminator1, discriminator2) = discriminators
    val (jobId0, jobId1, jobId2) = jobIds

    val drmOutputDir = drmConfig.workDir.toAbsolutePath.render
    val finalOutputDir = path("out/job-outputs").toAbsolutePath.render

    val sixSpaces = "      "

    val singularityPrefix: String = (drmSystem, dockerParamsOpt) match {
      case (DrmSystem.Lsf, Some(dockerParams)) => s"singularity exec docker://${dockerParams.imageName} "
      case _ => ""
    }
    
    val header: String = drmSystem match {
      case DrmSystem.Uger => """|#$ -cwd
                                |
                                |source /broad/software/scripts/useuse
                                |reuse -q UGER
                                |reuse -q Java-1.8
                                |
                                |export PATH=/humgen/diabetes/users/dig/miniconda2/bin:$PATH
                                |source activate loamstream_v1.0
                                |
                                |i=$SGE_TASK_ID
                                |jobId=$JOB_ID""".stripMargin
                               
      case DrmSystem.Lsf => """|
                               |
                               |i=$LSB_JOBINDEX
                               |jobId=$LSB_JOBID""".stripMargin
    }
    
    // scalastyle:off line.size.limit
    s"""#!/bin/bash
${header}
${sixSpaces}
if [ $$i -eq 1 ]
then
${singularityPrefix}/some/shapeit/executable -V /some/vcf/file.$discriminator0 -M /some/map/file.$discriminator0 -O /some/haplotype/file.$discriminator0 /some/sample/file -L /some/log/file --thread 2

LOAMSTREAM_JOB_EXIT_CODE=$$?

stdoutDestPath="$finalOutputDir/${jobId0}.stdout"
stderrDestPath="$finalOutputDir/${jobId0}.stderr"

mkdir -p $finalOutputDir
mv $drmOutputDir/${jobName}.1.stdout $$stdoutDestPath || echo "Couldn't move DRM std out log" > $$stdoutDestPath
mv $drmOutputDir/${jobName}.1.stderr $$stderrDestPath || echo "Couldn't move DRM std err log" > $$stderrDestPath

exit $$LOAMSTREAM_JOB_EXIT_CODE

elif [ $$i -eq 2 ]
then
${singularityPrefix}/some/shapeit/executable -V /some/vcf/file.$discriminator1 -M /some/map/file.$discriminator1 -O /some/haplotype/file.$discriminator1 /some/sample/file -L /some/log/file --thread 2

LOAMSTREAM_JOB_EXIT_CODE=$$?

stdoutDestPath="$finalOutputDir/${jobId1}.stdout"
stderrDestPath="$finalOutputDir/${jobId1}.stderr"

mkdir -p $finalOutputDir
mv $drmOutputDir/${jobName}.2.stdout $$stdoutDestPath || echo "Couldn't move DRM std out log" > $$stdoutDestPath
mv $drmOutputDir/${jobName}.2.stderr $$stderrDestPath || echo "Couldn't move DRM std err log" > $$stderrDestPath

exit $$LOAMSTREAM_JOB_EXIT_CODE

elif [ $$i -eq 3 ]
then
${singularityPrefix}/some/shapeit/executable -V /some/vcf/file.$discriminator2 -M /some/map/file.$discriminator2 -O /some/haplotype/file.$discriminator2 /some/sample/file -L /some/log/file --thread 2

LOAMSTREAM_JOB_EXIT_CODE=$$?

stdoutDestPath="$finalOutputDir/${jobId2}.stdout"
stderrDestPath="$finalOutputDir/${jobId2}.stderr"

mkdir -p $finalOutputDir
mv $drmOutputDir/${jobName}.3.stdout $$stdoutDestPath || echo "Couldn't move DRM std out log" > $$stdoutDestPath
mv $drmOutputDir/${jobName}.3.stderr $$stderrDestPath || echo "Couldn't move DRM std err log" > $$stderrDestPath

exit $$LOAMSTREAM_JOB_EXIT_CODE

fi
"""
  // scalastyle:on line.size.limit
  }
  // scalastyle:on method.length
}

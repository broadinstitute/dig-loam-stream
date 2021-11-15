package loamstream.drm

import java.nio.file.Path

import org.scalatest.FunSuite

import loamstream.TestHelpers
import loamstream.conf.DrmConfig
import loamstream.conf.ExecutionConfig
import loamstream.drm.lsf.LsfPathBuilder
import loamstream.drm.lsf.LsfScriptBuilderParams
import loamstream.drm.uger.UgerPathBuilder
import loamstream.drm.uger.UgerScriptBuilderParams
import loamstream.model.execute.LocalSettings
import loamstream.model.jobs.JobOracle
import loamstream.model.jobs.LJob
import loamstream.model.jobs.commandline.CommandLineJob
import loamstream.model.quantities.CpuTime
import loamstream.model.quantities.Cpus
import loamstream.model.quantities.Memory
import loamstream.util.BashScript.Implicits.BashPath
import loamstream.drm.slurm.SlurmPathBuilder
import loamstream.drm.slurm.SlurmScriptBuilderParams

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
  import loamstream.TestHelpers.path

  for {
    drmSystem <- DrmSystem.values
    containerParamsOpt <- Iterable(None, Some(ContainerParams("library/foo:1.23", "--foo --bar")))
  } {
    val testName = {
      s"A shell script is generated out of a CommandLineJob, and can be used to submit a ${drmSystem} job " +
      s"with container params $containerParamsOpt"
    }
    
    test(testName) { 
      val drmSettings = drmSystem.settingsMaker(
          Cpus(1),
          Memory.inGb(42),
          CpuTime.inHours(2),
          defaultQueue(drmSystem),
          containerParamsOpt)
  
      val discriminators @ (d0, d1, d2) = (0, 1, 2)
          
      val jobs = Seq(getShapeItCommandLineJob(d0), getShapeItCommandLineJob(d1), getShapeItCommandLineJob(d2))
      val jobName = DrmTaskArray.makeJobName()
      
      val config = drmConfig(drmSystem)
      
      def makePath(s: String): Path = path(s"foo/bar/baz/$s")
      
      val jobOracle: JobOracle = new JobOracle {
        override def dirOptFor(job: LJob): Option[Path] = Some(makePath(job.id.toString))
      }
      
      val taskArray = DrmTaskArray.fromCommandLineJobs(
          ExecutionConfig.default,
          jobOracle,
          drmSettings,
          config, 
          pathBuilder(drmSystem), 
          jobs, 
          jobName)
          
      val scriptContents = {
        (new ScriptBuilder(scriptBuilderParams(drmSystem))).buildFrom(taskArray).withNormalizedLineBreaks
      }
  
      val jobIds: (String, String, String) = (jobs(0).id.toString, jobs(1).id.toString, jobs(2).id.toString)
      
      val expectedScriptContents = expectedScriptAsString(
        makePath,
        config, 
        jobName, 
        discriminators, 
        jobIds, 
        drmSystem, 
        containerParamsOpt).withNormalizedLineBreaks
        
      assert(scriptContents === expectedScriptContents)
    }
  }
  
  private val ugerScriptBuilderParams = new UgerScriptBuilderParams(path("/foo/bar"), "someEnv")
  
  private val ugerPathBuilder = new UgerPathBuilder(ugerScriptBuilderParams)
  
  private def defaultQueue(drmSystem: DrmSystem): Option[Queue] = drmSystem match {
    case DrmSystem.Lsf | DrmSystem.Slurm => None
    case DrmSystem.Uger => Some(Queue("broad"))
    
  }
  
  private def drmConfig(drmSystem: DrmSystem): DrmConfig = drmSystem match {
    case DrmSystem.Lsf => TestHelpers.configWithLsf.lsfConfig.get
    case DrmSystem.Uger => TestHelpers.configWithUger.ugerConfig.get
    case DrmSystem.Slurm => TestHelpers.configWithSlurm.slurmConfig.get
  }
  
  private def pathBuilder(drmSystem: DrmSystem): PathBuilder = drmSystem match {
    case DrmSystem.Lsf => LsfPathBuilder
    case DrmSystem.Uger => ugerPathBuilder
    case DrmSystem.Slurm => SlurmPathBuilder
  }
  
  private def scriptBuilderParams(drmSystem: DrmSystem): ScriptBuilderParams = drmSystem match {
    case DrmSystem.Lsf => LsfScriptBuilderParams
    case DrmSystem.Uger => ugerScriptBuilderParams
    case DrmSystem.Slurm => SlurmScriptBuilderParams
  }

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

    CommandLineJob(commandLineString, shapeItWorkDir, LocalSettings, nameOpt = Some(discriminator.toString))
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
      makeJobDir: String => Path,
      drmConfig: DrmConfig,
      jobName: String, 
      discriminators: (Int, Int, Int), 
      jobIds: (String, String, String),
      drmSystem: DrmSystem, 
      containerParamsOpt: Option[ContainerParams]): String = {
    
    val (discriminator0, discriminator1, discriminator2) = {
      val (a, b, c) = discriminators
      
      //NB: Appease Scala 2.12.9 :\
      (a.toString, b.toString, c.toString)
    }
    val (jobId0, jobId1, jobId2) = jobIds

    val drmOutputDir = drmConfig.workDir.toAbsolutePath.render
    val finalOutputDir0 = makeJobDir(jobId0).toAbsolutePath.render
    val finalOutputDir1 = makeJobDir(jobId1).toAbsolutePath.render
    val finalOutputDir2 = makeJobDir(jobId2).toAbsolutePath.render

    val sixSpaces = "      "

    val singularityPrefix: String = containerParamsOpt match {
      case Some(ContainerParams(imageName, "")) => s"singularity exec ${imageName} "
      case Some(ContainerParams(imageName, params)) => s"singularity exec $params ${imageName} "
      case _ => ""
    }
    
    val header: String = drmSystem match {
      case DrmSystem.Uger => """|#$ -cwd
                                |
                                |source /broad/software/scripts/useuse
                                |reuse -q UGER
                                |reuse -q Java-1.8
                                |
                                |export PATH=/foo/bar:$PATH
                                |source activate someEnv
                                |
                                |mkdir -p /broad/hptmp/${USER}
                                |export SINGULARITY_CACHEDIR=/broad/hptmp/${USER}
                                |
                                |i=$SGE_TASK_ID
                                |jobId=$JOB_ID""".stripMargin
                               
      case DrmSystem.Lsf => """|
                               |
                               |i=$LSB_JOBINDEX
                               |jobId=$LSB_JOBID""".stripMargin
                               
      case DrmSystem.Slurm => """|module load singularity/3.2.1
                                 |
                                 |i=$SLURM_ARRAY_TASK_ID
                                 |jobId=$SLURM_ARRAY_JOB_ID""".stripMargin
    }

    def toIndex(i: Int): Int = DrmTaskArray.TaskIndexingStrategy.forDrmSystem(drmSystem).toIndex(i)
    
    // scalastyle:off line.size.limit
    s"""#!/bin/bash
${header}
${sixSpaces}
if [ "$$i" = "${toIndex(0)}" ]
then
jobDir="$finalOutputDir0"
mkdir -p "$$jobDir"

STATS_FILE="${finalOutputDir0}/stats"
EXITCODE_FILE="${finalOutputDir0}/exitcode"
COMMAND_SCRIPT="${finalOutputDir0}/command.sh"
STDOUT_FILE="${finalOutputDir0}/stdout"
STDERR_FILE="${finalOutputDir0}/stderr"

echo "Node: $$(hostname)" >> $$STATS_FILE
echo Task_Array_Name: ${jobName} >> $$STATS_FILE
echo DRM_Task_Id: "$${jobId}-$${i}" >> $$STATS_FILE
echo "Raw_Logs: .loamstream/${drmSystem.name.toLowerCase}/${jobName}/$${i}.{stdout,stderr}" >> $$STATS_FILE

START="$$(date +%Y-%m-%dT%H:%M:%S)"

`which time` -o $$STATS_FILE --format="ExitCode: %x\\nMemory: %Mk\\nSystem: %Ss\\nUser: %Us" bash $$COMMAND_SCRIPT 1> $$STDOUT_FILE 2> $$STDERR_FILE

LOAMSTREAM_JOB_EXIT_CODE=$$?

echo "$$LOAMSTREAM_JOB_EXIT_CODE" > $$EXITCODE_FILE

echo "Start: $$START" >> $$STATS_FILE
echo "End: $$(date +%Y-%m-%dT%H:%M:%S)" >> $$STATS_FILE

exit $$LOAMSTREAM_JOB_EXIT_CODE

elif [ "$$i" = "${toIndex(1)}" ]
then
jobDir="$finalOutputDir1"
mkdir -p "$$jobDir"

STATS_FILE="${finalOutputDir1}/stats"
EXITCODE_FILE="${finalOutputDir1}/exitcode"
COMMAND_SCRIPT="${finalOutputDir1}/command.sh"
STDOUT_FILE="${finalOutputDir1}/stdout"
STDERR_FILE="${finalOutputDir1}/stderr"

echo "Node: $$(hostname)" >> $$STATS_FILE
echo Task_Array_Name: ${jobName} >> $$STATS_FILE
echo DRM_Task_Id: "$${jobId}-$${i}" >> $$STATS_FILE
echo "Raw_Logs: .loamstream/${drmSystem.name.toLowerCase}/${jobName}/$${i}.{stdout,stderr}" >> $$STATS_FILE

START="$$(date +%Y-%m-%dT%H:%M:%S)"

`which time` -o $$STATS_FILE --format="ExitCode: %x\\nMemory: %Mk\\nSystem: %Ss\\nUser: %Us" bash $$COMMAND_SCRIPT 1> $$STDOUT_FILE 2> $$STDERR_FILE

LOAMSTREAM_JOB_EXIT_CODE=$$?

echo "$$LOAMSTREAM_JOB_EXIT_CODE" > $$EXITCODE_FILE

echo "Start: $$START" >> $$STATS_FILE
echo "End: $$(date +%Y-%m-%dT%H:%M:%S)" >> $$STATS_FILE

exit $$LOAMSTREAM_JOB_EXIT_CODE

elif [ "$$i" = "${toIndex(2)}" ]
then
jobDir="$finalOutputDir2"
mkdir -p "$$jobDir"

STATS_FILE="${finalOutputDir2}/stats"
EXITCODE_FILE="${finalOutputDir2}/exitcode"
COMMAND_SCRIPT="${finalOutputDir2}/command.sh"
STDOUT_FILE="${finalOutputDir2}/stdout"
STDERR_FILE="${finalOutputDir2}/stderr"

echo "Node: $$(hostname)" >> $$STATS_FILE
echo Task_Array_Name: ${jobName} >> $$STATS_FILE
echo DRM_Task_Id: "$${jobId}-$${i}" >> $$STATS_FILE
echo "Raw_Logs: .loamstream/${drmSystem.name.toLowerCase}/${jobName}/$${i}.{stdout,stderr}" >> $$STATS_FILE

START="$$(date +%Y-%m-%dT%H:%M:%S)"

`which time` -o $$STATS_FILE --format="ExitCode: %x\\nMemory: %Mk\\nSystem: %Ss\\nUser: %Us" bash $$COMMAND_SCRIPT 1> $$STDOUT_FILE 2> $$STDERR_FILE

LOAMSTREAM_JOB_EXIT_CODE=$$?

echo "$$LOAMSTREAM_JOB_EXIT_CODE" > $$EXITCODE_FILE

echo "Start: $$START" >> $$STATS_FILE
echo "End: $$(date +%Y-%m-%dT%H:%M:%S)" >> $$STATS_FILE

exit $$LOAMSTREAM_JOB_EXIT_CODE

fi
"""
  // scalastyle:on line.size.limit
  }
  // scalastyle:on method.length
}

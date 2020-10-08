package loamstream.model.execute

import java.net.URI
import java.nio.file.Files.exists
import java.time.LocalDateTime

import org.broadinstitute.dig.aws.JobStep
import org.broadinstitute.dig.aws.emr.AmiId
import org.broadinstitute.dig.aws.emr.ApplicationConfig
import org.broadinstitute.dig.aws.emr.ApplicationName
import org.broadinstitute.dig.aws.emr.BootstrapScript
import org.broadinstitute.dig.aws.emr.Cluster
import org.broadinstitute.dig.aws.emr.InstanceType
import org.scalatest.FunSuite

import loamstream.TestHelpers
import loamstream.drm.ContainerParams
import loamstream.drm.Queue
import loamstream.googlecloud.ClusterConfig
import loamstream.model.execute.Resources.AwsResources
import loamstream.model.execute.Resources.DrmResources
import loamstream.model.execute.Resources.GoogleResources
import loamstream.model.execute.Resources.LocalResources
import loamstream.model.execute.Resources.LsfResources
import loamstream.model.execute.Resources.UgerResources
import loamstream.model.jobs.Execution
import loamstream.model.jobs.JobOracle
import loamstream.model.jobs.JobResult
import loamstream.model.jobs.JobStatus
import loamstream.model.jobs.MockJob
import loamstream.model.quantities.CpuTime
import loamstream.model.quantities.Cpus
import loamstream.model.quantities.Memory
import loamstream.util.{ Files => LFiles }

/**
 * @author clint
 * May 28, 2019
 */
final class FileSystemExecutionRecorderTest extends FunSuite {
  import FileSystemExecutionRecorder.resourcesToString
  import FileSystemExecutionRecorder.settingsToString
  private val startTime = LocalDateTime.now
  
  test("record") {
    TestHelpers.withWorkDir(getClass.getSimpleName) { workDir =>
      val jobOracle: JobOracle = TestHelpers.InDirJobOracle(workDir)
      
      val job = MockJob(toReturn = JobStatus.Succeeded, name = "fooJob")
    
      val jobDir = jobOracle.dirFor(job)
      
      val cores = Cpus(42)
      val memoryPerCore = Memory.inGb(1.23)
      val maxRunTime = CpuTime.inSeconds(345)
      val queueOpt = Option(Queue("foo"))
      val containerParamsOpt = Option(ContainerParams("library/ubuntu:18.04"))
      
      val settings = UgerDrmSettings(cores, memoryPerCore, maxRunTime, queueOpt, containerParamsOpt)
      
      val cpuTime = CpuTime.inSeconds(345)
      val nodeOpt = Option("foo.example.com")
      val endTime = LocalDateTime.now
      val rawData = Option("fofofofo")
      
      val resources = UgerResources(memoryPerCore, cpuTime, nodeOpt, queueOpt, startTime, endTime, rawData)
      
      val execution = Execution(
          cmd = Some("foo --bar"),
          settings = settings,
          status = JobStatus.Succeeded,
          result = Some(JobResult.CommandResult(0)),
          resources = Some(resources),
          jobDir = Some(jobDir),
          terminationReason = None)
      
      assert(exists(jobDir) === false)
          
      FileSystemExecutionRecorder.record(jobOracle, Seq(job -> execution))
    
      assert(exists(jobDir) === true)
  
      val expectedSettingsFileContents = Seq(
          s"settings-type\tuger",
          s"singularity-image-name\t${settings.containerParams.map(_.imageName).getOrElse("")}",
          s"cores\t${settings.cores.value}",
          s"memory\t${settings.memoryPerCore.value}",
          s"max-run-time\t${settings.maxRunTime.duration}",
          s"queue\t${settings.queue.map(_.toString).getOrElse("")}").mkString("\n")
      
      val settingsFile = jobDir.resolve("settings")
      
      assert(LFiles.readFrom(settingsFile) === expectedSettingsFileContents)
          
      val expectedAccountingSummaryFileContents = Seq(
            s"start-time\t${resources.startTime}",
            s"end-time\t${resources.endTime}",
            s"memory\t${resources.memory.value}",
            s"cpu-time\t${resources.cpuTime.duration}",
            s"execution-node\t${resources.node.getOrElse("")}",
            s"queue\t${resources.queue.map(_.toString).getOrElse("")}").mkString("\n")
      
      val accountingSummaryFile = jobDir.resolve("accounting-summary")
      
      assert(LFiles.readFrom(accountingSummaryFile) === expectedAccountingSummaryFileContents)
      
      val accountingFile = jobDir.resolve("accounting")
      
      assert(LFiles.readFrom(accountingFile) === "fofofofo")
    }
  }
  
  test("settingsToString - local settings") {
    assert(settingsToString(LocalSettings) === s"settings-type\tlocal")
  }
  
  test("settingsToString - Google settings") {
    val expected = Seq(
        "settings-type" -> "google",
        "cluster" -> "foo",
        "zone" -> "us-central1-b",
        "master-machine-type" -> "n1-standard-1",
        "master-boot-disk-size" -> "20",
        "num-workers" -> "2",
        "worker-machine-type" -> "n1-standard-1",
        "worker-boot-disk-size" -> "20",
        "num-preemptible-workers" -> "0",
        "preemptible-worker-boot-disk-size" -> "20",
        "properties" -> "short-props",
        "max-cluster-idle-time" -> "10m").map { case (k, v) => s"${k}\t${v}" }.mkString("\n")
    
    val clusterConfig = ClusterConfig.default.copy(properties = "short-props")
  
    assert(settingsToString(GoogleSettings("foo", clusterConfig)) === expected)
  }
  
  test("settingsToString - AWS settings") {
    val appNames = Seq(ApplicationName("bar"), ApplicationName("baz"))
    
    val appConfigs = Seq(ApplicationConfig("some-classification"), ApplicationConfig("some-other-classification"))
    
    val bootstrapUri0 = URI.create("s3://some-bootstrap-script")
    val bootstrapUri1 = URI.create("s3://some-other-bootstrap-script")
    val bootstrapScripts = Seq(new BootstrapScript(bootstrapUri0), new BootstrapScript(bootstrapUri1))

    val scriptUri0 = URI.create("s3://some-script")
    val scriptUri1 = URI.create("s3://some-pyspark-script")
    val bootstrapSteps = Seq(JobStep.Script(scriptUri0), JobStep.PySpark(scriptUri1))
    
    val clusterConfig = Cluster(
        name = "foo",
        amiId = Some(AmiId("ami-some-ami-id")),
        instances = 42,
        masterInstanceType = InstanceType("some-master-instance-type"),
        slaveInstanceType = InstanceType("some-slave-instance-type"),
        masterVolumeSizeInGB = 12,
        slaveVolumeSizeInGB = 34,
        applications = appNames,
        configurations = appConfigs,
        bootstrapScripts = bootstrapScripts,
        bootstrapSteps = bootstrapSteps,
        keepAliveWhenNoSteps = true,
        visibleToAllUsers = false)
    
    val expected = Seq(
        "settings-type" -> "aws",
        "cluster" -> "foo",
        "ami-id" -> "ami-some-ami-id",
        "instances" -> "42",
        "master-instance-type" -> "some-master-instance-type",
        "slave-instance-type" -> "some-slave-instance-type",
        "master-volume-size-in-gb" -> "12",
        "slave-volume-size-in-gb" -> "34",
        "applications" -> "[bar,baz]",
        "configurations" -> appConfigs.mkString("[", ",", "]"),
        "bootstrap-scripts" -> Seq(bootstrapUri0, bootstrapUri1).mkString("[", ",", "]"),
        "bootstraps-steps" -> bootstrapSteps.mkString("[", ",", "]"),
        "keep-alive-when-no-steps" -> "true",
        "visible-to-all-users" -> "false"
        ).map { case (k, v) => s"${k}\t${v}" }.mkString("\n")
    
    assert(settingsToString(AwsSettings(clusterConfig)) === expected)
  }
  
  test("settingsToString - DRM settings") {
    def doTest(makeSettings: DrmSettings.SettingsMaker, expectedSettingsType: String): Unit = {
      def doTestWith(queueOpt: Option[Queue], containerParamsOpt: Option[ContainerParams]): Unit = {
        val cores = Cpus(42)
        val memoryPerCore = Memory.inGb(1.23)
        val maxRunTime = CpuTime.inSeconds(345)

        val settings = makeSettings(cores, memoryPerCore, maxRunTime, queueOpt, containerParamsOpt)
      
        val actual = settingsToString(settings)
      
        val expected = Seq(
            s"settings-type\t${expectedSettingsType}",
            s"singularity-image-name\t${settings.containerParams.map(_.imageName).getOrElse("")}",
            s"cores\t${settings.cores.value}",
            s"memory\t${settings.memoryPerCore.value}",
            s"max-run-time\t${settings.maxRunTime.duration}",
            s"queue\t${settings.queue.map(_.toString).getOrElse("")}").mkString("\n")
          
        assert(actual === expected)
      }
      
      val queue = Queue("foo")
      val containerParams = ContainerParams("some-image")
      
      doTestWith(None, None)
      doTestWith(Some(queue), None)
      doTestWith(None, Some(containerParams))
      doTestWith(Some(queue), Some(containerParams))
    }
    
    doTest(UgerDrmSettings.apply, "uger")
    doTest(LsfDrmSettings.apply, "lsf")
  }
  
  test("resourcesToString - local resources") {
    val endTime = LocalDateTime.now
    
    val resources = LocalResources(startTime, endTime)
    
    assert(resourcesToString(resources) === s"start-time\t${startTime}\nend-time\t${endTime}")
  }
  
  test("resourcesToString - google resources") {
    val endTime = LocalDateTime.now
    
    val resources = GoogleResources("foo", startTime, endTime)
    
    assert(resourcesToString(resources) === s"start-time\t${startTime}\nend-time\t${endTime}\ncluster\tfoo")
  }
  
  test("resourcesToString - AWS resources") {
    val endTime = LocalDateTime.now
    
    val resources = AwsResources("foo", startTime, endTime)
    
    assert(resourcesToString(resources) === s"start-time\t${startTime}\nend-time\t${endTime}\ncluster\tfoo")
  }
  
  test("resourcesToString - DRM resources") {
    val endTime = LocalDateTime.now
    
    def doTest[R <: DrmResources](makeResources: DrmResources.ResourcesMaker[R]): Unit = {
      def doTestWith(nodeOpt: Option[String], queueOpt: Option[Queue]): Unit = {
        val memoryPerCore = Memory.inGb(1.23)
        val cpuTime = CpuTime.inSeconds(345)
        
        val resources = makeResources(memoryPerCore, cpuTime, nodeOpt, queueOpt, startTime, endTime, Some("fofofofo"))
        
        val actual = resourcesToString(resources)
        
        val expected = Seq(
            s"start-time\t${resources.startTime}",
            s"end-time\t${resources.endTime}",
            s"memory\t${resources.memory.value}",
            s"cpu-time\t${resources.cpuTime.duration}",
            s"execution-node\t${resources.node.getOrElse("")}",
            s"queue\t${resources.queue.map(_.toString).getOrElse("")}").mkString("\n")
        
        assert(actual === expected)
      }
      
      val queue = Queue("foo")
      val node = "foo.example.com"
      
      doTestWith(None, None)
      doTestWith(None, Some(queue))
      doTestWith(Some(node), None)
      doTestWith(Some(node), Some(queue))
    }

    doTest(UgerResources.apply)
    doTest(LsfResources.apply)
  }
}

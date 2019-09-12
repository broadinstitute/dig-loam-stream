package loamstream.model.execute

import loamstream.model.jobs.Execution
import loamstream.model.jobs.LJob
import loamstream.model.jobs.JobOracle
import loamstream.drm.ContainerParams
import loamstream.model.execute.Resources.LocalResources
import loamstream.model.execute.Resources.GoogleResources
import loamstream.model.execute.Resources.DrmResources
import java.nio.file.{Files => JFiles}
import java.nio.file.Path
import loamstream.util.Paths
import loamstream.util.{Files => LFiles}
import loamstream.model.execute.Resources.AwsResources


/**
 * @author clint
 * May 22, 2019
 */
object FileSystemExecutionRecorder extends ExecutionRecorder {
  import FileSystemExecutionRecorder._
  
  override def record(jobOracle: JobOracle, executionTuples: Iterable[(LJob, Execution)]): Unit = {
    for {
      (job, execution) <- executionTuples
    } {
      val jobDir = jobOracle.dirFor(job)
      
      JFiles.createDirectories(jobDir)
      
      val settingsFilePath = makeSettingsFilePath(jobDir)
      
      LFiles.writeTo(settingsFilePath)(settingsToString(execution.settings))
      
      for {
        rs <- execution.resources
        accountingSummaryFile = makeAccountingSummaryFilePath(jobDir)
        _ = LFiles.writeTo(accountingSummaryFile)(resourcesToString(rs))
        rawResourceData <- rs.raw
      } {
        val accountingFile = makeAccountingFilePath(jobDir)
        
        LFiles.writeTo(accountingFile)(rawResourceData)
      }
    }
  }

  private def pathWithExtension(dir: Path, fileName: String): Path = dir.resolve(fileName).toAbsolutePath
  
  private[execute] def makeSettingsFilePath(dir: Path): Path = pathWithExtension(dir, "settings")
  
  private[execute] def makeAccountingFilePath(dir: Path): Path = pathWithExtension(dir, "accounting")
  
  private[execute] def makeAccountingSummaryFilePath(dir: Path): Path = pathWithExtension(dir, "accounting-summary")
  
  private object Keys {
    val settingsType = "settings-type"
    val startTime = "start-time"
    val endTime = "end-time"
    // Uger/LSF
    val memory = "memory"
    val cores = "cores"
    val maxRunTime = "max-run-time"
    val cpuTime = "cpu-time"
    val executionNode = "execution-node"
    val queue = "queue"
    val singularityImageName = "singularity-image-name"
    val metadata = "metadata"
    val zone = "zone"
    // Google
    val cluster = "cluster"
    val masterMachineType = "master-machine-type"
    val masterBootDiskSize = "master-boot-disk-size"
    val numWorkers = "num-workers"
    val workerMachineType = "worker-machine-type"
    val workerBootDiskSize = "worker-boot-disk-size"
    val numPreemptibleWorkers = "num-preemptible-workers"
    val preemptibleWorkerBootDiskSize = "preemptible-worker-boot-disk-size"
    val imageVersion = "image-version"
    val scopes = "scopes"
    val properties = "properties"
    val initializationActions = "initialization-actions"
    val maxClusterIdleTime = "max-cluster-idle-time"
    // AWS
    val amiId = "ami-id"
    val instances = "instances"
    val masterInstanceType = "master-instance-type"
    val slaveInstanceType = "slave-instance-type"
    val masterVolumeSizeInGB = "master-volume-size-in-gb"
    val slaveVolumeSizeInGB = "slave-volume-size-in-gb"
    val applications = "applications"
    val configurations = "configurations"
    val bootstrapScripts = "bootstrap-scripts"
    val bootstrapSteps = "bootstraps-steps"
    val keepAliveWhenNoSteps = "keep-alive-when-no-steps"
    val visibleToAllUsers = "visible-to-all-users"
  }
  
  private def line(key: String, value: String): String = s"${key}\t${value}"
  
  private def tuplesToFileContents(ts: Iterable[(String, Any)]): String = {
    ts.map { case (k, v) => line(k, v.toString) }.mkString("\n") 
  }
  
  def resourcesToString(resources: Resources): String = tuplesToFileContents(resourcesToTuples(resources))
  
  def settingsToString(settings: Settings): String = tuplesToFileContents(settingsToTuples(settings))
  
  def resourcesToTuples(resources: Resources): Iterable[(String, Any)] = {
    val startAndEndTimeTuples : Seq[(String, Any)] = {
      Seq(
        Keys.startTime -> resources.startTime.toString,
        Keys.endTime -> resources.endTime.toString)
    }
    
    val resourceSpecificTuples: Seq[(String, Any)] = resources match {
      case _: LocalResources => Nil
      case g: GoogleResources => Seq(Keys.cluster -> g.clusterId)
      case a: AwsResources => Seq(Keys.cluster -> a.clusterId)
      case DrmResources(memory, cpuTime, nodeOpt, queueOpt, _, _, _) => {
        Seq(
          Keys.memory -> memory.value.toString,
          Keys.cpuTime -> cpuTime.duration.toString,
          Keys.executionNode -> nodeOpt.getOrElse(""),
          Keys.queue -> queueOpt.map(_.toString).getOrElse(""))
      }
    }
    
    startAndEndTimeTuples ++ resourceSpecificTuples
  }

  def settingsToTuples(settings: Settings): Seq[(String, Any)] = {
    def typeTuple(envTypeName: String): (String, Any) = Keys.settingsType -> envTypeName
    
    settings match {
      case LocalSettings => Seq(typeTuple(EnvironmentType.Local.name))
      case GoogleSettings(cluster, clusterConfig) => {
        import clusterConfig._
        
        Seq(
          typeTuple(EnvironmentType.Google.name), 
          Keys.cluster -> cluster,
          Keys.zone -> zone,
          Keys.masterMachineType -> masterMachineType,
          Keys.masterBootDiskSize -> masterBootDiskSize,
          Keys.numWorkers -> numWorkers,
          Keys.workerMachineType -> workerMachineType,
          Keys.workerBootDiskSize -> workerBootDiskSize,
          Keys.numPreemptibleWorkers -> numPreemptibleWorkers,
          Keys.preemptibleWorkerBootDiskSize -> preemptibleWorkerBootDiskSize,
          Keys.properties -> properties,
          Keys.maxClusterIdleTime -> maxClusterIdleTime)
      }
      case AwsSettings(clusterConfig) => {
        import clusterConfig._
        
        def toString[A](as: Iterable[A]): String = as.mkString("[", ",", "]")
        
        Seq(
          typeTuple(EnvironmentType.Aws.name),
          Keys.cluster -> name,
          Keys.amiId -> amiId.map(_.value).getOrElse(""),
          Keys.instances -> instances,
          Keys.masterInstanceType -> masterInstanceType.value,
          Keys.slaveInstanceType -> slaveInstanceType.value,
          Keys.masterVolumeSizeInGB -> masterVolumeSizeInGB,
          Keys.slaveVolumeSizeInGB -> slaveVolumeSizeInGB,
          Keys.applications -> toString(applications.map(_.value)),
          Keys.configurations -> toString(configurations),
          Keys.bootstrapScripts -> toString(bootstrapScripts.map(_.config.scriptBootstrapAction.path)),
          Keys.bootstrapSteps -> toString(bootstrapSteps),
          Keys.keepAliveWhenNoSteps -> keepAliveWhenNoSteps,
          Keys.visibleToAllUsers -> visibleToAllUsers)
      }
      case DrmSettings(cores, memory, cpuTime, queueOpt, containerParamsOpt) => {
        val containerParamsTuples = Seq(
          Keys.singularityImageName -> containerParamsOpt.map(_.imageName).getOrElse(""))
        
        (typeTuple(settings.envType.name) +: containerParamsTuples) ++ Seq(
          Keys.cores -> cores.value.toString,
          Keys.memory -> memory.value.toString,
          Keys.maxRunTime -> cpuTime.duration.toString,
          Keys.queue -> queueOpt.map(_.toString).getOrElse("")) 
      }
    }
  }
}

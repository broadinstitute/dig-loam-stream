package loamstream.model.execute

import loamstream.model.jobs.Execution
import loamstream.model.jobs.LJob
import loamstream.model.jobs.JobOracle
import loamstream.drm.ContainerParams
import loamstream.model.execute.Resources.LocalResources
import loamstream.model.execute.Resources.GoogleResources
import loamstream.model.execute.Resources.DrmResources
import java.nio.file.Path
import loamstream.util.Paths
import loamstream.util.Files

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
      
      java.nio.file.Files.createDirectories(jobDir)
      
      val settingsFilePath = makeSettingsFilePath(jobDir)
      
      Files.writeTo(settingsFilePath)(settingsToString(execution.settings))
      
      for {
        rs <- execution.resources
        accountingSummaryFile = makeAccountingSummaryFilePath(jobDir)
        _ = Files.writeTo(accountingSummaryFile)(resourcesToString(rs))
        rawResourceData <- rs.raw
      } {
        val accountingFile = makeAccountingFilePath(jobDir)
        
        Files.writeTo(accountingFile)(rawResourceData)
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
    val cluster = "cluster"
    val memory = "memory"
    val cores = "cores"
    val maxRunTime = "max-run-time"
    val cpuTime = "cpu-time"
    val executionNode = "execution-node"
    val queue = "queue"
    val singularityImageName = "singularity-image-name"
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
      case g: GoogleResources => Seq(Keys.cluster -> g.cluster)
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
      case GoogleSettings(cluster) => {
        Seq(
          typeTuple(EnvironmentType.Google.name), 
          Keys.cluster -> cluster)
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

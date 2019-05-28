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
  override def record(jobOracle: JobOracle, executionTuples: Iterable[(LJob, Execution)]): Unit = {
    for {
      (job, execution) <- executionTuples
    } {
      val jobDir = jobOracle.dirFor(job)
      
      java.nio.file.Files.createDirectories(jobDir)
      
      val settingsFilePath = makeSettingsFilePath(job, jobDir)
      
      Files.writeTo(settingsFilePath)(settingsToString(execution.settings))
      
      execution.resources.foreach { rs =>
        val accountingFilePath = FileSystemExecutionRecorder.makeAccountingFilePath(job, jobDir)
      
        Files.writeTo(accountingFilePath)(resourcesToString(rs))
      }
    }
  }

  private def pathWithExtension(extension: String, job: LJob, dir: Path): Path = {
    dir.resolve(s"${Paths.mungePathRelatedChars(job.name)}.${extension}")
  }
  
  private[execute] def makeSettingsFilePath(job: LJob, dir: Path): Path = pathWithExtension("settings", job, dir)
  
  private[execute] def makeAccountingFilePath(job: LJob, dir: Path): Path = pathWithExtension("accounting", job, dir)
  
  private object SettingsKeys {
    val settingsType = "settings-type"
  }
  
  def line(key: String, value: String): String = s"${key}\t${value}"
  
  def resourcesToString(resources: Resources): String = {
    def startAndEndTimeLines: Seq[String] = {
      Seq(
        line("start-time", resources.startTime.toString),
        line("end-time", resources.endTime.toString))
    }
    
    val resourceSpecificLines: Seq[String] = resources match {
      case _: LocalResources => Nil
      case g: GoogleResources => Seq(line("cluster", g.cluster))
      case DrmResources(memory, cpuTime, nodeOpt, queueOpt, _, _) => {
        Seq(
          line("memory", memory.toString),
          line("cpu-time", cpuTime.duration.toString),
          line("execution-node", nodeOpt.getOrElse("")),
          line("queue", queueOpt.map(_.toString).getOrElse("")))
      }
    }
    
    (startAndEndTimeLines ++ resourceSpecificLines).mkString("\n")
  }
  
  def settingsToString(settings: Settings): String = {
    def typeLine(envTypeName: String): String = line(SettingsKeys.settingsType, envTypeName)
    
    def linesFrom(containerParams: ContainerParams): Seq[String] = {
      Seq(line("singularity-image-name", containerParams.imageName))
    }
    
    def emptyContainerParamsLine: String = line("singularity-image-name", "")
    
    settings match {
      case LocalSettings => typeLine(EnvironmentType.Local.name)
      case GoogleSettings(cluster) => {
        Seq(typeLine(EnvironmentType.Google.name), line("cluster", cluster)).mkString("\n")
      }
      case DrmSettings(cpus, memory, cpuTime, queueOpt, containerParamsOpt) => {
        val tpe = settings.envType match {
          case et @ (EnvironmentType.Uger | EnvironmentType.Lsf) => et.name
          case et => sys.error(s"DRM Environment type expected, but got $et")
        }
        
        Seq(
          typeLine(tpe),
          line("cpus", cpus.value.toString),
          line("memory", memory.value.toString),
          line("cpu-time", cpuTime.duration.toString),
          line("queue", queueOpt.map(_.toString).getOrElse("")),
          containerParamsOpt.map(linesFrom).getOrElse(Seq(emptyContainerParamsLine))).mkString("\n")
      }
    }
  }
}

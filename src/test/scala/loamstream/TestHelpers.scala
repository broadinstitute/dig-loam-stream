package loamstream

import java.nio.file.Files
import java.nio.file.Path
import java.time.Instant

import scala.concurrent.duration.Duration

import com.typesafe.config.ConfigFactory

import loamstream.compiler.LoamCompiler
import loamstream.compiler.LoamEngine
import loamstream.conf.ExecutionConfig
import loamstream.conf.LoamConfig
import loamstream.conf.PythonConfig
import loamstream.conf.RConfig
import loamstream.conf.UgerConfig
import loamstream.googlecloud.GoogleCloudConfig
import loamstream.googlecloud.HailConfig
import loamstream.loam.LoamGraph
import loamstream.loam.LoamProjectContext
import loamstream.loam.LoamScriptContext
import loamstream.model.execute.Resources
import loamstream.model.execute.Resources.LocalResources
import loamstream.model.execute.RxExecuter
import loamstream.model.jobs.Execution
import loamstream.model.jobs.JobResult
import loamstream.model.jobs.JobStatus
import loamstream.model.jobs.LJob
import loamstream.model.jobs.StoreRecord
import loamstream.util.Sequence
import loamstream.model.execute.DrmSettings
import loamstream.util.Paths
import org.apache.commons.io.FileUtils
import loamstream.model.jobs.OutputStreams
import java.util.UUID
import loamstream.model.jobs.RunData
import scala.concurrent.Future
import scala.concurrent.Await
import loamstream.loam.LoamScript
import loamstream.drm.uger.UgerDefaults
import loamstream.model.execute.Resources.UgerResources
import loamstream.model.quantities.Memory
import loamstream.model.quantities.CpuTime
import loamstream.model.execute.Resources.LsfResources
import loamstream.drm.Queue
import loamstream.model.execute.Resources.GoogleResources
import loamstream.conf.LsfConfig
import loamstream.drm.DrmSystem
import loamstream.model.execute.UgerDrmSettings
import loamstream.model.execute.LsfDrmSettings
import loamstream.conf.CompilationConfig
import loamstream.model.jobs.TerminationReason
import loamstream.model.execute.Settings
import loamstream.model.execute.LocalSettings
import loamstream.model.jobs.JobOracle
import loamstream.model.execute.EnvironmentType
import java.time.LocalDateTime
import java.time.ZoneId
import loamstream.conf.LsSettings

/**
  * @author clint
  *         date: Mar 10, 2016
  */
object TestHelpers {
  object DummyJobOracle extends JobOracle {
    override def dirOptFor(job: LJob): Option[Path] = Some(path(Paths.mungePathRelatedChars(job.name)))
  }
  
  final case class InDirJobOracle(dir: Path) extends JobOracle {
    override def dirOptFor(job: LJob): Option[Path] = Some(dir.resolve(Paths.mungePathRelatedChars(job.name)))
  }
  
  def path(p: String): Path = java.nio.file.Paths.get(p)

  val approxDoublePrecision: Double = 1e-16
  val graceFactor: Int = 20
  val tolerance: Double = graceFactor * approxDoublePrecision

  def areWithinExpectedError(x: Double, y: Double): Boolean = (x - y) / Math.max(x.abs, y.abs) < tolerance
  
  lazy val config: LoamConfig = {
    val config = ConfigFactory.load("loamstream-test")
    
    val ugerConfig = UgerConfig.fromConfig(config)
    val lsfConfig = LsfConfig.fromConfig(config)
    val googleConfig = GoogleCloudConfig.fromConfig(config)
    val hailConfig = HailConfig.fromConfig(config)
    val pythonConfig = PythonConfig.fromConfig(config)
    val rConfig = RConfig.fromConfig(config)
    val executionConfig = ExecutionConfig.fromConfig(config)
    val compilationConfig = CompilationConfig.fromConfig(config)

    LoamConfig( 
      ugerConfig.toOption,
      lsfConfig.toOption,
      googleConfig.toOption,
      hailConfig.toOption,
      pythonConfig.toOption,
      rConfig.toOption,
      executionConfig.getOrElse(ExecutionConfig.default),
      compilationConfig.getOrElse(CompilationConfig.default))
  }
  
  lazy val configWithUger = config.copy(drmSystem = Option(DrmSystem.Uger))
  lazy val configWithLsf = config.copy(drmSystem = Option(DrmSystem.Lsf))
  
  lazy val localResources: LocalResources = { 
    val now = LocalDateTime.now
      
    LocalResources(now, now)
  }
  
  val broadQueue = Queue("broad")
  
  def toLocalDateTime(epochMilli: Long): LocalDateTime = {
    Instant.ofEpochMilli(epochMilli).atZone(ZoneId.systemDefault).toLocalDateTime
  }
  
  lazy val ugerResources: UgerResources = {
    val mem = Memory.inGb(2.1)
    val cpu = CpuTime.inSeconds(12.34)
    val startTime = toLocalDateTime(64532) // scalastyle:ignore magic.number
    val endTime = toLocalDateTime(9345345) // scalastyle:ignore magic.number

    UgerResources(mem, cpu, Some("nodeName"), Some(broadQueue), startTime, endTime)
  }
  
  lazy val lsfResources: LsfResources = {
    val mem = Memory.inGb(1.2)
    val cpu = CpuTime.inSeconds(43.21)
    val startTime = toLocalDateTime(64532) // scalastyle:ignore magic.number
    val endTime = toLocalDateTime(9345345) // scalastyle:ignore magic.number

    LsfResources(mem, cpu, Some("nodeName"), Some(broadQueue), startTime, endTime)
  }
  
  lazy val googleResources: GoogleResources = GoogleResources("some-cluster-id", LocalDateTime.now, LocalDateTime.now)

  def runDataFrom(
      job: LJob,
      settings: Settings,
      status: JobStatus,
      result: Option[JobResult] = None,
      resources: Option[Resources] = None,
      jobDir: Option[Path] = None,
      terminationReasonOpt: Option[TerminationReason] = None): RunData = {
    
    RunData(job, settings, status, result, resources, jobDir, terminationReasonOpt)
  }
  
  def executionFrom(status: JobStatus,
                    result: Option[JobResult] = None,
                    resources: Option[Resources] = None,
                    jobDir: Option[Path] = None): Execution = {
    Execution(
        settings = LocalSettings,
        cmd = None,
        status = status,
        result = result,
        resources = resources,
        outputs = Set.empty[StoreRecord],
        jobDir = jobDir,
        terminationReason = None)
  }

  def runDataFromStatus(
      job: LJob, 
      settings: Settings, 
      status: JobStatus, 
      resources: Option[Resources] = None): RunData = runDataFrom(job, settings, status, result = None, resources)
  
  def executionFromStatus(status: JobStatus, resources: Option[Resources] = None): Execution = {
    executionFrom(status, result = None, resources)
  }

  def runDataFromResult(
      job: LJob,
      settings: Settings,
      result: JobResult, 
      resources: Option[Resources] = None): RunData = {
    
    runDataFrom(job, settings, result.toJobStatus, Option(result), resources)
  }
  
  def executionFromResult(result: JobResult, resources: Option[Resources] = None): Execution = {
    executionFrom(result.toJobStatus, Option(result), resources)
  }
  
  def emptyProjectContext: LoamProjectContext = emptyProjectContext(LsSettings.noCliConfig)
  
  def emptyProjectContext(drmSystem: DrmSystem): LoamProjectContext = {
    LoamProjectContext.empty(config.copy(drmSystem = Option(drmSystem)), LsSettings.noCliConfig)
  }
  
  def emptyProjectContext(lsSettings: LsSettings): LoamProjectContext = LoamProjectContext.empty(config, lsSettings)
  
  def withScriptContext[A](f: LoamScriptContext => A): A = f(new LoamScriptContext(emptyProjectContext))
  
  def withScriptContext[A](lsSettings: LsSettings)(f: LoamScriptContext => A): A = {
    f(new LoamScriptContext(emptyProjectContext(lsSettings)))
  }
  
  def withScriptContext[A](drmSystem: DrmSystem)(f: LoamScriptContext => A): A = {
    f(new LoamScriptContext(emptyProjectContext(drmSystem)))
  }
  
  def makeGraph(loamCode: LoamScriptContext => Any): LoamGraph = {
    withScriptContext { sc =>
      loamCode(sc)
      
      sc.projectContext.graph
    }
  }
  
  def makeGraph(drmSystem: DrmSystem)(loamCode: LoamScriptContext => Any): LoamGraph = {
    withScriptContext(drmSystem) { sc =>
      loamCode(sc)
      
      sc.projectContext.graph
    }
  }
  
  def makeGraph(lsSettings: LsSettings)(loamCode: LoamScriptContext => Any): LoamGraph = {
    withScriptContext(lsSettings) { sc =>
      loamCode(sc)
      
      sc.projectContext.graph
    }
  }
  
  def run(graph: LoamGraph, timeout: Duration = Duration.Inf): Map[LJob, Execution] = {
    val executable = LoamEngine.toExecutable(graph)
    
    RxExecuter.default.execute(executable)(timeout)
  }
  
  def getWorkDir(basename: String, deleteAtJvmShutdown: Boolean = true): Path = {
    val result = Files.createTempDirectory(basename)

    if(deleteAtJvmShutdown) {
      //NB: This seems very heavy-handed, but java.io.File.deleteOnExit doesn't work for non-empty directories. :\
      Runtime.getRuntime.addShutdownHook(new Thread {
        override def run(): Unit = FileUtils.deleteQuietly(result.toFile)
      })
    }
    
    result
  }
  
  def withWorkDir[A](basename: String)(body: Path => A): A = {
    val workDir = getWorkDir(basename, deleteAtJvmShutdown = false)
    
    try { body(workDir) }
    finally { /*FileUtils.deleteQuietly(workDir.toFile)*/ }
  }

  def loamEngine: LoamEngine = LoamEngine.default(config, LsSettings.noCliConfig)

  def compile(loamCode: String): LoamCompiler.Result = {
    loamEngine.compiler.compile(config, LsSettings.noCliConfig, LoamScript.withGeneratedName(loamCode))
  }
  
  val defaultUgerSettings: UgerDrmSettings = {
    val ugerConfig = config.ugerConfig.get 

    UgerDrmSettings(
      ugerConfig.defaultCores,
      ugerConfig.defaultMemoryPerCore,
      ugerConfig.defaultMaxRunTime,
      Option(UgerDefaults.queue),
      None)
  }
  
  val defaultLsfSettings: LsfDrmSettings = {
    val lsfConfig = config.lsfConfig.get 

    LsfDrmSettings(
      lsfConfig.defaultCores,
      lsfConfig.defaultMemoryPerCore,
      lsfConfig.defaultMaxRunTime,
      None,
      None)
  }
  
  def dummyJobDir: Path = path(UUID.randomUUID.toString).toAbsolutePath
  
  def dummyFileName: Path = path(s"${UUID.randomUUID.toString}.log")
    
  def dummyOutputStreams: OutputStreams = OutputStreams(dummyFileName, dummyFileName)
  
  def waitFor[A](f: Future[A]): A = {
    import scala.concurrent.duration._

    //NB: Hard-coded timeout. :(  But at least it's no longer infinite! :)
    Await.result(f, 10.minutes)
  }
  
  //foobar => FoObAr, etc
  def to1337Speak(s: String): String = {
    val pairs = s.toUpperCase.zip(s.toLowerCase)
    val z: (String, Boolean) = ("", true)
    
    val (result, _) = pairs.foldLeft(z) { (accTuple, pair) =>
      val (acc, upper) = accTuple
      val (uc, lc) = pair
      val newLetter = if(upper) uc else lc
      val newAcc = acc :+ newLetter
      newAcc -> !upper
    }
    
    result
  }
}

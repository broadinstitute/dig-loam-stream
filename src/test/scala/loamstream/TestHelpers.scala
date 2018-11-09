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
import loamstream.model.execute.Environment
import loamstream.model.execute.Resources
import loamstream.model.execute.Resources.LocalResources
import loamstream.model.execute.RxExecuter
import loamstream.model.jobs.Execution
import loamstream.model.jobs.JobResult
import loamstream.model.jobs.JobStatus
import loamstream.model.jobs.LJob
import loamstream.model.jobs.OutputRecord
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

/**
  * @author clint
  *         date: Mar 10, 2016
  */
object TestHelpers {
  def path(p: String): Path = java.nio.file.Paths.get(p)

  val approxDoublePrecision: Double = 1e-16
  val graceFactor: Int = 20
  val tolerance: Double = graceFactor * approxDoublePrecision

  val alwaysRestart: LJob => Boolean = _ => true
  val neverRestart: LJob => Boolean = _ => false
  
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
    val now = Instant.now
      
    LocalResources(now, now)
  }
  
  val broadQueue = Queue("broad")
  
  lazy val ugerResources: UgerResources = {
    val mem = Memory.inGb(2.1)
    val cpu = CpuTime.inSeconds(12.34)
    val startTime = Instant.ofEpochMilli(64532) // scalastyle:ignore magic.number
    val endTime = Instant.ofEpochMilli(9345345) // scalastyle:ignore magic.number

    UgerResources(mem, cpu, Some("nodeName"), Some(broadQueue), startTime, endTime)
  }
  
  lazy val lsfResources: LsfResources = {
    val mem = Memory.inGb(1.2)
    val cpu = CpuTime.inSeconds(43.21)
    val startTime = Instant.ofEpochMilli(64532) // scalastyle:ignore magic.number
    val endTime = Instant.ofEpochMilli(9345345) // scalastyle:ignore magic.number

    LsfResources(mem, cpu, Some("nodeName"), Some(broadQueue), startTime, endTime)
  }
  
  lazy val googleResources: GoogleResources = GoogleResources("some-cluster-id", Instant.now, Instant.now)

  val env: Environment = Environment.Local

  def runDataFrom(
      job: LJob,
      status: JobStatus,
      result: Option[JobResult] = None,
      resources: Option[Resources] = None,
      outputStreams: Option[OutputStreams] = None): RunData = RunData(job, status, result, resources, outputStreams)
  
  def executionFrom(status: JobStatus,
                    result: Option[JobResult] = None,
                    resources: Option[Resources] = None,
                    outputStreams: Option[OutputStreams] = None): Execution = {
    Execution(
        env = env,
        cmd = None,
        status,
        result,
        resources,
        Set.empty[OutputRecord],
        outputStreams)
  }

  def runDataFromStatus(job: LJob, status: JobStatus, resources: Option[Resources] = None): RunData = {
    runDataFrom(job, status, result = None, resources)
  }
  
  def executionFromStatus(status: JobStatus, resources: Option[Resources] = None): Execution = {
    executionFrom(status, result = None, resources)
  }

  def runDataFromResult(job: LJob, result: JobResult, resources: Option[Resources] = None): RunData = {
    runDataFrom(job, result.toJobStatus, Option(result), resources)
  }
  
  def executionFromResult(result: JobResult, resources: Option[Resources] = None): Execution = {
    executionFrom(result.toJobStatus, Option(result), resources)
  }
  
  def emptyProjectContext = LoamProjectContext.empty(config)
  
  def emptyProjectContext(drmSystem: DrmSystem) = LoamProjectContext.empty(config.copy(drmSystem = Option(drmSystem)))
  
  def withScriptContext[A](f: LoamScriptContext => A): A = f(new LoamScriptContext(emptyProjectContext))
  
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
    finally { FileUtils.deleteQuietly(workDir.toFile) }
  }

  def loamEngine: LoamEngine = LoamEngine.default(config)

  def compile(loamCode: String): LoamCompiler.Result = {
    loamEngine.compiler.compile(config, LoamScript.withGeneratedName(loamCode))
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
  
  def dummyFileName: Path = TestHelpers.path(s"${UUID.randomUUID.toString}.log")
    
  def dummyOutputStreams: OutputStreams = OutputStreams(dummyFileName, dummyFileName)
  
  def waitFor[A](f: Future[A]): A = {
    import scala.concurrent.duration._

    //NB: Hard-coded timeout. :(  But at least it's no longer infinite! :)
    Await.result(f, 10.minutes)
  }
}

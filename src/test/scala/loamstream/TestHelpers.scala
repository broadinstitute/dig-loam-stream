package loamstream

import java.nio.file.{Path, Paths}

import loamstream.conf.{LoamConfig, PythonConfig, RConfig, UgerConfig}
import com.typesafe.config.ConfigFactory
import loamstream.googlecloud.GoogleCloudConfig
import loamstream.googlecloud.HailConfig
import loamstream.model.execute.Resources.LocalResources
import java.time.Instant

import loamstream.model.execute.{ExecutionEnvironment, LocalSettings, Resources}
import loamstream.model.jobs.{Execution, JobResult, JobStatus, OutputRecord}
import loamstream.model.jobs.LJob
import loamstream.conf.ExecutionConfig
import loamstream.loam.LoamScriptContext
import loamstream.loam.LoamGraph
import loamstream.loam.LoamProjectContext
import loamstream.model.execute.RxExecuter
import loamstream.compiler.LoamEngine
import loamstream.util.Sequence
import java.nio.file.Files
import scala.concurrent.duration.Duration

/**
  * @author clint
  *         date: Mar 10, 2016
  */
object TestHelpers {
  def path(p: String): Path = Paths.get(p)

  val approxDoublePrecision = 1e-16
  val graceFactor = 20
  val tolerance = graceFactor * approxDoublePrecision

  val alwaysRestart: LJob => Boolean = _ => true
  val neverRestart: LJob => Boolean = _ => false
  
  def areWithinExpectedError(x: Double, y: Double): Boolean = (x - y) / Math.max(x.abs, y.abs) < tolerance
  
  lazy val config: LoamConfig = {
    val config = ConfigFactory.load("loamstream-test")
    
    val ugerConfig = UgerConfig.fromConfig(config)
    val googleConfig = GoogleCloudConfig.fromConfig(config)
    val hailConfig = HailConfig.fromConfig(config)
    val pythonConfig = PythonConfig.fromConfig(config)
    val rConfig = RConfig.fromConfig(config)
    val executionConfig = ExecutionConfig.fromConfig(config)

    LoamConfig( 
      ugerConfig.toOption,
      googleConfig.toOption,
      hailConfig.toOption,
      pythonConfig.toOption,
      rConfig.toOption,
      executionConfig.getOrElse(ExecutionConfig.default))
  }
  
  lazy val localResources: LocalResources = { 
    val now = Instant.now
      
    LocalResources(now, now)
  }

  val env = ExecutionEnvironment.Local

  def executionFrom(status: JobStatus,
                    result: Option[JobResult] = None,
                    resources: Option[Resources] = None): Execution = {

    Execution(id = None,
              env,
              cmd = None,
              settings = LocalSettings(),
              status,
              result,
              resources,
              Set.empty[OutputRecord])
  }

  def executionFromStatus(status: JobStatus, resources: Option[Resources] = None): Execution = {
    executionFrom(status, result = None, resources)
  }

  def executionFromResult(result: JobResult, resources: Option[Resources] = None): Execution = {
    executionFrom(result.toJobStatus, Option(result), resources)
  }
  
  def emptyProjectContext = LoamProjectContext.empty(config)
  
  def makeGraph(loamCode: LoamScriptContext => Any): LoamGraph = {
      
    val sc = new LoamScriptContext(emptyProjectContext)
      
    loamCode(sc)
      
    sc.projectContext.graph
  }
  
  def run(graph: LoamGraph, timeout: Duration = Duration.Inf): Map[LJob, Execution] = {
    val executable = LoamEngine.toExecutable(graph)
    
    RxExecuter.default.execute(executable)(timeout)
  }
  
  def getWorkDir(basename: String): Path = {
    val suffixes = Sequence[Int]()
    
    val candidates = suffixes.iterator.map(i => path(s"target/$basename-$i"))
    
    val exists: Path => Boolean = Files.exists(_)
    
    candidates.dropWhile(exists).next()
  }
}

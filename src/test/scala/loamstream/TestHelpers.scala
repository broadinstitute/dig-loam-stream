package loamstream

import java.nio.file.{Path, Paths}

import loamstream.conf.LoamConfig
import com.typesafe.config.ConfigFactory
import loamstream.conf.UgerConfig
import loamstream.googlecloud.GoogleCloudConfig
import loamstream.googlecloud.HailConfig
import loamstream.model.execute.Resources.LocalResources
import java.time.Instant

import loamstream.model.execute.{ExecutionEnvironment, LocalSettings}
import loamstream.model.jobs.{Execution, JobResult, JobStatus, OutputRecord}

/**
  * @author clint
  *         date: Mar 10, 2016
  */
object TestHelpers {
  def path(p: String): Path = Paths.get(p)

  val approxDoublePrecision = 1e-16
  val graceFactor = 20
  val tolerance = graceFactor * approxDoublePrecision

  def areWithinExpectedError(x: Double, y: Double): Boolean = (x - y) / Math.max(x.abs, y.abs) < tolerance
  
  lazy val config: LoamConfig = {
    val config = ConfigFactory.load("loamstream-test")
    
    val ugerConfig = UgerConfig.fromConfig(config)
    val googleConfig = GoogleCloudConfig.fromConfig(config)
    val hailConfig = HailConfig.fromConfig(config)
    
    LoamConfig(ugerConfig.toOption, googleConfig.toOption, hailConfig.toOption)
  }
  
  lazy val localResources: LocalResources = { 
    val now = Instant.now
      
    LocalResources(now, now)
  }

  val env = ExecutionEnvironment.Local

  def executionFrom(jobStatus: JobStatus = JobStatus.Succeeded,
                    jobResult: JobResult = JobResult.CommandResult(0)) = {
    Execution(env,
      cmd = None,
      settings = LocalSettings(),
      jobStatus,
      Option(jobResult),
      resources = None,
      Set.empty[OutputRecord])
  }
}

package loamstream.uger

import com.typesafe.config.ConfigFactory
import loamstream.conf.UgerConfig
import loamstream.model.jobs.NoOpJob
import loamstream.uger.JobStatus.{Done, Queued, Running}
import org.scalatest.FunSuite

import scala.concurrent.ExecutionContext
import scala.util.Success
import loamstream.util.Futures
import loamstream.util.ObservableEnrichments

/**
  * Created by kyuksel on 7/25/16.
  */
final class UgerChunkRunnerTest extends FunSuite {
  private val config = UgerConfig.fromConfig(ConfigFactory.load("loamstream-test.conf")).get
  private val client = MockDrmaaClient(Success(Queued), Success(Running), Success(Done))
  private val runner = UgerChunkRunner(config, client)

  import Futures.waitFor
  import ObservableEnrichments._
  
  test("NoOpJob is not attempted to be executed") {
    val noOpJob = NoOpJob(Set.empty)
    val result = runner.run(Set(noOpJob))(ExecutionContext.global)
    
    assert(waitFor(result.lastAsFuture) === Success(Map()))
  }

  test("No failures when empty set of jobs is presented") {
    val result = runner.run(Set.empty)(ExecutionContext.global)
    
    assert(waitFor(result.lastAsFuture) === Success(Map()))
  }
}

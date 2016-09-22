package loamstream.uger

import com.typesafe.config.ConfigFactory
import loamstream.conf.UgerConfig
import loamstream.model.jobs.NoOpJob
import loamstream.uger.JobStatus.{Done, Queued, Running}
import org.scalatest.FunSuite

import scala.concurrent.ExecutionContext
import scala.util.Success

/**
  * Created by kyuksel on 7/25/16.
  */
class UgerChunkRunnerTest extends FunSuite {
  val config = UgerConfig.fromConfig(ConfigFactory.load("loamstream-test.conf")).get
  val client = MockDrmaaClient(Success(Queued), Success(Running), Success(Done))
  val runner = UgerChunkRunner(config, client)

  test("NoOpJob is not attempted to be executed") {
    val noOpJob = NoOpJob(Set.empty)
    val result = runner.run(Set(noOpJob))(ExecutionContext.global)
    assert(result.value === Some(Success(Map())))
  }

  test("No failures when empty set of jobs is presented") {
    val result = runner.run(Set.empty)(ExecutionContext.global)
    assert(result.value === Some(Success(Map())))
  }
}

package loamstream.uger

import scala.concurrent.Await
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.util.Success

import org.scalatest.FunSuite

import com.typesafe.config.ConfigFactory

import loamstream.conf.UgerConfig
import loamstream.model.jobs.NoOpJob
import java.nio.file.Paths

/**
  * Created by kyuksel on 7/25/16.
  */
final class UgerChunkRunnerTest extends FunSuite {
  //scalastyle:off magic.number
  
  val config = UgerConfig(Paths.get("target/foo"), Paths.get("target/bar"), 42)
  val client = MockDrmaaClient(Map.empty)
  val runner = UgerChunkRunner(config, client)

  //TODO: Replace with Futures.waitFor
  private def waitFor[A](f: Future[A]): A = Await.result(f, Duration.Inf)
  
  test("NoOpJob is not attempted to be executed") {
    val noOpJob = NoOpJob(Set.empty)
    val result = waitFor(runner.run(Set(noOpJob))(ExecutionContext.global))
    assert(result === Map.empty)
  }

  test("No failures when empty set of jobs is presented") {
    val result = waitFor(runner.run(Set.empty)(ExecutionContext.global))
    assert(result === Map.empty)
  }
  
  test("combine") {
    import UgerChunkRunner.combine
    
    assert(combine(Map.empty, Map.empty) == Map.empty)
    
    val m1 = Map("a" -> 1, "b" -> 2, "c" -> 3)
    
    assert(combine(Map.empty, m1) == Map.empty)
    
    assert(combine(m1, Map.empty) == Map.empty)
    
    val m2 = Map("a" -> 42.0, "c" -> 99.0, "x" -> 123.456)
    
    assert(combine(m1, m2) == Map("a" -> (1, 42.0), "c" -> (3, 99.0)))
    
    assert(combine(m2, m1) == Map("a" -> (42.0, 1), "c" -> (99.0, 3)))
  }
  
  //scalastyle:on magic.number
}

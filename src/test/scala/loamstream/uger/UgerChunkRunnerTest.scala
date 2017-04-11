package loamstream.uger

import java.nio.file.Paths
import loamstream.conf.UgerConfig
import loamstream.model.jobs.NoOpJob
import loamstream.util.Futures
import loamstream.util.ObservableEnrichments
import org.scalatest.FunSuite
import rx.lang.scala.schedulers.IOScheduler
import loamstream.TestHelpers

/**
  * Created by kyuksel on 7/25/16.
  */
final class UgerChunkRunnerTest extends FunSuite {
  //scalastyle:off magic.number
  
  private val scheduler = IOScheduler()
  
  import TestHelpers.neverRestart
  
  private val config = UgerConfig(Paths.get("target/foo"), Paths.get("target/bar"), "some job parameters", 42)
  private val client = MockDrmaaClient(Map.empty)
  private val runner = UgerChunkRunner(config, client, new JobMonitor(scheduler, Poller.drmaa(client)))

  import Futures.waitFor
  import ObservableEnrichments._
  
  test("NoOpJob is not attempted to be executed") {
    val noOpJob = NoOpJob(Set.empty)
    val result = waitFor(runner.run(Set(noOpJob), neverRestart).firstAsFuture)
    assert(result === Map.empty)
  }

  test("No failures when empty set of jobs is presented") {
    val result = waitFor(runner.run(Set.empty, neverRestart).firstAsFuture)
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

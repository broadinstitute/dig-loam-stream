package loamstream.apps.minimal

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration

import loamstream.model.execute.AbstractExecuterTest
import loamstream.model.jobs.LJob.SimpleFailure

/**
 * @author clint
 * date: Apr 12, 2016
 */
final class MiniExecuterTest extends AbstractExecuterTest {

  test("executeLeaf()") {
    import MiniExecuter.executeLeaf
    
    val success = Await.result(executeLeaf(two0), Duration.Inf)
    
    assert(success === Map(two0 -> two0Success))
    
    val failure = Await.result(executeLeaf(two0Failed), Duration.Inf)
    
    assert(failure === Map.empty)
  }
  
  test("noFailures()") {
    import MiniExecuter.noFailures

    assert(noFailures(Map.empty) === true)

    val allSuccesses = Map(
      two0 -> two0Success,
      two1 -> two1Success,
      twoPlusTwo -> twoPlusTwoSuccess,
      plusOne -> plusOneSuccess)
      
    assert(noFailures(allSuccesses) === true)
    
    val allFailures = Map(
      two0 -> SimpleFailure("foo"),
      two1 -> SimpleFailure("bar"),
      twoPlusTwo -> SimpleFailure("baz"),
      plusOne -> SimpleFailure("blerg"))
      
    assert(noFailures(allFailures) === false)
    
    val someFailures = Map(
      two0 -> two0Success,
      two1 -> SimpleFailure("bar"),
      twoPlusTwo -> twoPlusTwoSuccess,
      plusOne -> SimpleFailure("blerg"))
      
    assert(noFailures(someFailures) === false)
  }
}
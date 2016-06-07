package loamstream.apps.minimal

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration

import loamstream.model.execute.ExecuterTest
import loamstream.model.execute.LExecuter
import loamstream.model.jobs.LJob
import loamstream.model.jobs.LJob.Result
import loamstream.model.jobs.LJob.SimpleFailure

/**
 * @author clint
 * date: Apr 12, 2016
 */
final class MiniExecuterTest extends ExecuterTest {
  
  override def makeExecuter: LExecuter = MiniExecuter
  
  test("execSingle()") {
    import MiniExecuter.execSingle
    
    val success = Await.result(execSingle(two0), Duration.Inf)
    
    assert(success === Map(two0 -> two0Success))
    
    val failure = Await.result(execSingle(two0Failed), Duration.Inf)
    
    assert(failure === Map(two0Failed -> two0Failure))
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
  
  test("consumeUntilFirstFailure()") {
    import MiniExecuter.consumeUntilFirstFailure
    
    assert(consumeUntilFirstFailure(Iterator.empty) == Vector.empty)
    
    val oneSuccess: Map[LJob, Result] = Map(two0 -> two0Success)
    val anotherSuccess: Map[LJob, Result] = Map(two1 -> two1Success)
    
    val oneFailure: Map[LJob, Result] = Map(two0Failed -> two0Failure)
    val anotherFailure: Map[LJob, Result] = Map(two1Failed -> two1Failure)
    
    assert(consumeUntilFirstFailure(Iterator(oneSuccess)) == Vector(oneSuccess))
    
    assert(consumeUntilFirstFailure(Iterator(oneSuccess, anotherSuccess)) == Vector(oneSuccess, anotherSuccess))
    
    assert(consumeUntilFirstFailure(Iterator(oneFailure)) == Vector(oneFailure))
    
    assert(consumeUntilFirstFailure(Iterator(oneFailure, anotherFailure)) == Vector(oneFailure))
    
    assert(consumeUntilFirstFailure(Iterator(oneSuccess, oneFailure)) == Vector(oneSuccess, oneFailure))
    
    assert(consumeUntilFirstFailure(Iterator(oneSuccess, anotherSuccess, oneFailure)) == 
      Vector(oneSuccess, anotherSuccess, oneFailure))
    
    assert(consumeUntilFirstFailure(Iterator(oneSuccess, anotherSuccess, oneFailure, anotherFailure)) == 
      Vector(oneSuccess, anotherSuccess, oneFailure))
  }
}
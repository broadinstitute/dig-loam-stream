package loamstream.model.execute

import org.scalatest.FunSuite
import scala.concurrent.Await
import loamstream.model.jobs.TestJobs
import scala.concurrent.duration.Duration
import loamstream.model.jobs.LJob.SimpleFailure
import loamstream.model.jobs.LJob
import loamstream.model.jobs.LJob.Result

/**
 * @author clint
 * date: Jun 7, 2016
 */
final class ExecuterHelpersTest extends FunSuite with TestJobs {
  
  test("execSingle()") {
    import ExecuterHelpers.executeSingle
    import scala.concurrent.ExecutionContext.Implicits.global
    
    val success = Await.result(executeSingle(two0), Duration.Inf)
    
    assert(success === Map(two0 -> two0Success))
    
    val failure = Await.result(executeSingle(two0Failed), Duration.Inf)
    
    assert(failure === Map(two0Failed -> two0Failure))
  }
  
  test("noFailures()") {
    import ExecuterHelpers.noFailures

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
    import ExecuterHelpers.consumeUntilFirstFailure
    
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
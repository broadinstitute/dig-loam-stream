package loamstream.model.execute

import org.scalatest.FunSuite
import scala.concurrent.Await
import loamstream.model.jobs.TestJobs
import scala.concurrent.duration.Duration
import loamstream.model.jobs.JobResult
import loamstream.model.jobs.RxMockJob

/**
 * @author clint
 * date: Jun 7, 2016
 */
final class ExecuterHelpersTest extends FunSuite with TestJobs {
  
  test("flattenTree") {
    import ExecuterHelpers.flattenTree
    
    val noDeps0 = RxMockJob("noDeps")
    
    assert(flattenTree(Set(noDeps0)) == Set(noDeps0))
    
    val middle0 = RxMockJob("middle", Set(noDeps0))
    
    assert(flattenTree(Set(middle0)) == Set(middle0, noDeps0))
    
    val root0 = RxMockJob("root", Set(middle0))
    
    assert(flattenTree(Set(root0)) == Set(root0, middle0, noDeps0))
    
    val noDeps1 = RxMockJob("noDeps1")
    val middle1 = RxMockJob("middle1", Set(noDeps1))
    val root1 = RxMockJob("root1", Set(middle1))
    
    assert(flattenTree(Set(root0, root1)) == Set(root0, middle0, noDeps0, root1, middle1, noDeps1))
  }
  
  test("execSingle()") {
    import ExecuterHelpers.executeSingle
    import scala.concurrent.ExecutionContext.Implicits.global
    
    val success = Await.result(executeSingle(two0), Duration.Inf)
    
    assert(success === Map(two0 -> two0Success))
    
    val failure = Await.result(executeSingle(two0Failed), Duration.Inf)
    
    assert(failure === Map(two0Failed -> two0Failure))
  }
  
  test("noFailures() and anyFailures()") {
    import ExecuterHelpers.{noFailures,anyFailures}

    assert(noFailures(Map.empty) === true)
    assert(anyFailures(Map.empty) === false)

    val allSuccesses = Map(
      two0 -> two0Success,
      two1 -> two1Success,
      twoPlusTwo -> twoPlusTwoSuccess,
      plusOne -> plusOneSuccess)
      
    assert(noFailures(allSuccesses) === true)
    assert(anyFailures(allSuccesses) === false)
    
    val allFailures = Map(
      two0 -> JobResult.Failed(),
      two1 -> JobResult.Failed(),
      twoPlusTwo -> JobResult.Failed(),
      plusOne -> JobResult.Failed())
      
    assert(noFailures(allFailures) === false)
    assert(anyFailures(allFailures) === true)
    
    val someFailures = Map(
      two0 -> two0Success,
      two1 -> JobResult.Failed(),
      twoPlusTwo -> twoPlusTwoSuccess,
      plusOne -> JobResult.Failed())
      
    assert(noFailures(someFailures) === false)
    assert(anyFailures(someFailures) === true)
  }
}

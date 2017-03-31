package loamstream.model.execute

import loamstream.TestHelpers
import org.scalatest.FunSuite

import scala.concurrent.Await
import loamstream.model.jobs.{JobStatus, RxMockJob, TestJobs}

import scala.concurrent.duration.Duration

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
    import TestHelpers.executionFromStatus
    import scala.concurrent.ExecutionContext.Implicits.global
    
    val success = Await.result(executeSingle(two0), Duration.Inf)
    
    assert(success === Map(two0 -> executionFromStatus(two0Success)))
    
    val failure = Await.result(executeSingle(two0Failed), Duration.Inf)
    
    assert(failure === Map(two0Failed -> executionFromStatus(two0Failure)))
  }
  
  test("noFailures() and anyFailures()") {
    import ExecuterHelpers.{noFailures,anyFailures}

    assert(noFailures(Map.empty) === true)
    assert(anyFailures(Map.empty) === false)

    val allSuccesses = Map( two0 -> two0Success,
                            two1 -> two1Success,
                            twoPlusTwo -> twoPlusTwoSuccess,
                            plusOne -> plusOneSuccess).mapValues(TestHelpers.executionFrom(_))
      
    assert(noFailures(allSuccesses) === true)
    assert(anyFailures(allSuccesses) === false)
    
    val allFailures = Map(
                          two0 -> JobStatus.Failed,
                          two1 -> JobStatus.Failed,
                          twoPlusTwo -> JobStatus.Failed,
                          plusOne -> JobStatus.Failed).mapValues(TestHelpers.executionFrom(_))
      
    assert(noFailures(allFailures) === false)
    assert(anyFailures(allFailures) === true)
    
    val someFailures = Map(
                            two0 -> two0Success,
                            two1 -> JobStatus.Failed,
                            twoPlusTwo -> twoPlusTwoSuccess,
                            plusOne -> JobStatus.Failed).mapValues(TestHelpers.executionFrom(_))
      
    assert(noFailures(someFailures) === false)
    assert(anyFailures(someFailures) === true)
  }
}

package loamstream.apps.minimal

import scala.concurrent.Await
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration.Duration

import org.scalatest.FunSuite

import loamstream.model.execute.LExecutable
import loamstream.model.jobs.LJob
import loamstream.model.jobs.LJob.Result
import loamstream.model.jobs.LJob.SimpleFailure
import loamstream.model.jobs.LJob.SimpleSuccess
import loamstream.util.Hit

/**
 * @author clint
 * date: Apr 12, 2016
 */
final class MiniExecuterTest extends FunSuite {
  private final case class MockLJob(inputs: Set[LJob], toReturn: LJob.Result) extends LJob {
    override def execute(implicit context: ExecutionContext): Future[Result] = Future.successful(toReturn)
    
    override def withInputs(newInputs: Set[LJob]): LJob = copy(inputs = newInputs) 
  }

  private val two0Success = SimpleSuccess("2(0)")
  private val two1Success = SimpleSuccess("2(1)")

  private val twoPlusTwoSuccess = SimpleSuccess("2 + 2")

  private val plusOneSuccess = SimpleSuccess("(2 + 2) + 1")
  
  private val two0Failure = SimpleFailure("2(0)")
  private val two1Failure = SimpleFailure("2(1)")

  private val twoPlusTwoFailure = SimpleFailure("2 + 2")

  private val plusOneFailure = SimpleFailure("(2 + 2) + 1")

  private val two0 = MockLJob(Set.empty, two0Success)
  private val two1 = MockLJob(Set.empty, two1Success)

  private val twoPlusTwo = MockLJob(Set(two0, two1), twoPlusTwoSuccess)

  private val plusOne = MockLJob(Set(twoPlusTwo), plusOneSuccess)
  
  private val two0Failed = MockLJob(Set.empty, two0Failure)
  private val two1Failed = MockLJob(Set.empty, two1Failure)

  private val twoPlusTwoFailed = MockLJob(Set(two0Failed, two1Failed), twoPlusTwoFailure)

  private val plusOneFailed = MockLJob(Set(twoPlusTwoFailed), plusOneFailure)

  import scala.concurrent.ExecutionContext.Implicits.global
  
  test("execute() should work if all sub-jobs succeed") {

    val executable = LExecutable(Set(plusOne))

    val result = MiniExecuter.execute(executable)

    val expected = Map(
      two0 -> Hit(two0Success),
      two1 -> Hit(two1Success),
      twoPlusTwo -> Hit(twoPlusTwoSuccess),
      plusOne -> Hit(plusOneSuccess))

    assert(result === expected)
  }
  
  test("execute() should work if no sub-jobs succeed") {

    val executable = LExecutable(Set(plusOneFailed))

    val result = MiniExecuter.execute(executable)

    val expected = Map.empty

    assert(result === expected)
  }
  
  test("execute() should work if some (early) sub-jobs fail") {

    val twoPlusTwo = MockLJob(Set(two0Failed, two1Failed), twoPlusTwoFailure)

    val plusOne = MockLJob(Set(twoPlusTwo), plusOneFailure)
    
    val executable = LExecutable(Set(plusOneFailed))

    val result = MiniExecuter.execute(executable)

    val expected = Map.empty

    assert(result === expected)
  }
  
  test("execute() should work if some (late) sub-jobs fail") {

    val twoPlusTwo = MockLJob(Set(two0, two1), twoPlusTwoFailure)

    val plusOne = MockLJob(Set(twoPlusTwo), plusOneFailure)
    
    val executable = LExecutable(Set(plusOne))

    val result = MiniExecuter.execute(executable)

    val expected = Map(
        two0 -> Hit(two0Success),
        two1 -> Hit(two1Success))

    assert(result === expected)
  }
  
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
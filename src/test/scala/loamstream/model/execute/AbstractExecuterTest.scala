package loamstream.model.execute

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import org.scalatest.FunSuite

import ch.qos.logback.core.util.Duration
import loamstream.apps.minimal.MiniExecuter
import loamstream.model.jobs.LJob
import loamstream.model.jobs.LJob.Result
import loamstream.model.jobs.LJob.SimpleFailure
import loamstream.model.jobs.LJob.SimpleSuccess
import loamstream.util.Hit
import loamstream.model.jobs.MockLJob
import loamstream.model.jobs.TestJobs


/**
 * @author clint
 * date: Jun 2, 2016
 */
abstract class AbstractExecuterTest(implicit executionContext: ExecutionContext) extends FunSuite with TestJobs { 

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
}
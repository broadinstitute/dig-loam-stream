package loamstream.model.execute

import scala.concurrent.ExecutionContext

import org.scalatest.FunSuite

import loamstream.model.jobs.LJob
import loamstream.model.jobs.MockJob
import loamstream.model.jobs.TestJobs
import loamstream.util.Hit
import loamstream.model.jobs.JobResult


/**
 * @author clint
 * date: Jun 2, 2016
 */
abstract class ExecuterTest(implicit executionContext: ExecutionContext) extends FunSuite with TestJobs { 

  def makeExecuter: Executer
  
  def withExecuter(testName: String)(f: Executer => Any): Unit = {
    test(testName) {
      f(makeExecuter)
    }
  }
  
  withExecuter("single jobs should work") { executer =>
    def doTest(job: LJob, expectedResult: JobResult): Unit = {
      val executable = Executable(Set(job))
  
      val result = executer.execute(executable)
  
      val expected = Map(job -> Hit(expectedResult))
  
      assert(result === expected)
    }
    
    doTest(two0, two0Success)
    doTest(two1, two1Success)
  }
  
  withExecuter("Jobs with one level of dependencies should work") { executer =>
    val executable = Executable(Set(twoPlusTwo))
  
    val result = executer.execute(executable)
    
    val expected = Map(
      two0 -> Hit(two0Success),
      two1 -> Hit(two1Success),
      twoPlusTwo -> Hit(twoPlusTwoSuccess))
      
    assert(result == expected)
  }
  
  withExecuter("Jobs with one level of dependencies where one dep fails") { executer =>
    val twoPlusTwoWithFailure = twoPlusTwo.copy(inputs = Set(two0, two1Failed), toReturn = twoPlusTwoFailure)
    
    val executable = Executable(Set(twoPlusTwoWithFailure))
  
    val result = executer.execute(executable)
    
    val expected0 = Map(
      two0 -> Hit(two0Success),
      two1Failed -> Hit(two1Failure),
      twoPlusTwoWithFailure -> Hit(twoPlusTwoFailure))
      
    assert(result == expected0)
  }
  
  withExecuter("execute() should work if all sub-jobs succeed") { executer =>

    val executable = Executable(Set(plusOne))

    val result = executer.execute(executable)

    val expected = Map(
      two0 -> Hit(two0Success),
      two1 -> Hit(two1Success),
      twoPlusTwo -> Hit(twoPlusTwoSuccess),
      plusOne -> Hit(plusOneSuccess))

    assert(result === expected)
  }
  
  withExecuter("execute() should work if no sub-jobs succeed") { executer =>

    val executable = Executable(Set(plusOneFailed))

    val result = executer.execute(executable)

    val alwaysExpected = Map(
      twoPlusTwoFailed -> Hit(twoPlusTwoFailure),
      plusOneFailed -> Hit(plusOneFailure))
    
    val expected0 = alwaysExpected + (two0Failed -> Hit(two0Failure))
    val expected1 = alwaysExpected + (two1Failed -> Hit(two1Failure))
    
    assert(result == expected0 || result == expected1)
  }
  
  withExecuter("execute() should work if some (early) sub-jobs fail") { executer =>

    val twoPlusTwo = MockJob(twoPlusTwoFailure, inputs = Set(two0Failed, two1Failed))

    val plusOne = MockJob(plusOneFailure, inputs = Set(twoPlusTwo))
    
    val executable = Executable(Set(plusOne))

    val result = executer.execute(executable)
    
    val alwaysExpected = Map(
      twoPlusTwo -> Hit(twoPlusTwoFailure),
      plusOne -> Hit(plusOneFailure))
    
    val expected0 = alwaysExpected + (two0Failed -> Hit(two0Failure))
    val expected1 = alwaysExpected + (two1Failed -> Hit(two1Failure))

    assert(result == expected0 || result == expected1)
  }
  
  withExecuter("execute() should work if some (late) sub-jobs fail") { executer =>

    val twoPlusTwo = MockJob(twoPlusTwoFailure, inputs = Set(two0, two1))

    val plusOne = MockJob(plusOneFailure, inputs = Set(twoPlusTwo))
    
    val executable = Executable(Set(plusOne))

    val result = executer.execute(executable)

    val expected = Map(
        two0 -> Hit(two0Success),
        two1 -> Hit(two1Success),
        twoPlusTwo -> Hit(twoPlusTwoFailure),
        plusOne -> Hit(plusOneFailure))

    assert(result === expected)
  }
}
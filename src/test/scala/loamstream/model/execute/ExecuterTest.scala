package loamstream.model.execute

import loamstream.TestHelpers

import scala.concurrent.ExecutionContext
import org.scalatest.FunSuite
import loamstream.model.jobs.{JobStatus, LJob, MockJob, TestJobs}
import rx.lang.scala.Observable

/**
 * @author clint
 * date: Jun 2, 2016
 */
abstract class ExecuterTest(implicit executionContext: ExecutionContext) extends FunSuite with TestJobs { 

  def makeExecuter: Executer

  def withExecuter(testName: String)(body: => Any): Unit = {
    test(testName) {
      body
    }
  }

  def executeJobsAndMapToStatuses(jobs: Set[LJob]) = {
    val executer = makeExecuter

    executer.execute(Observable.from(jobs)).mapValues(_.status)
  }
  
  withExecuter("single jobs should work") {
    def doTest(job: LJob, expectedStatus: JobStatus): Unit = {
      val status = executeJobsAndMapToStatuses(Set(job))
  
      val expected = Map(job -> expectedStatus)
  
      assert(status === expected)
    }
    
    doTest(two0, two0Success)
    doTest(two1, two1Success)
  }
  
  withExecuter("Jobs with one level of dependencies should work") {
    val status = executeJobsAndMapToStatuses(Set(twoPlusTwo))

    val expected = Map(
      two0 -> two0Success,
      two1 -> two1Success,
      twoPlusTwo -> twoPlusTwoSuccess)
      
    assert(status === expected)
  }
  
  withExecuter("Jobs with one level of dependencies where one dep fails") {
    val twoPlusTwoWithFailure = twoPlusTwo.copy(inputs = Set(two0, two1Failed),
                                                toReturn = TestHelpers.executionFrom(twoPlusTwoFailure))

    val status = executeJobsAndMapToStatuses(Set(twoPlusTwoWithFailure))
  
    val expected0 = Map(
      two0 -> two0Success,
      two1Failed -> two1Failure,
      twoPlusTwoWithFailure -> twoPlusTwoFailure)
      
    assert(status == expected0)
  }
  
  withExecuter("execute() should work if all sub-jobs succeed") {

    val status = executeJobsAndMapToStatuses(Set(plusOne))

    val expected = Map(
      two0 -> two0Success,
      two1 -> two1Success,
      twoPlusTwo -> twoPlusTwoSuccess,
      plusOne -> plusOneSuccess)

    assert(status === expected)
  }
  
  withExecuter("execute() should work if no sub-jobs succeed") {

    val status = executeJobsAndMapToStatuses(Set(plusOneFailed))

    val alwaysExpected = Map(
      twoPlusTwoFailed -> twoPlusTwoFailure,
      plusOneFailed -> plusOneFailure)
    
    val expected0 = alwaysExpected + (two0Failed -> two0Failure)
    val expected1 = alwaysExpected + (two1Failed -> two1Failure)
    
    assert(status == expected0 || status == expected1)
  }
  
  withExecuter("execute() should work if some (early) sub-jobs fail") {

    val twoPlusTwo = MockJob(twoPlusTwoFailure, inputs = Set(two0Failed, two1Failed))

    val plusOne = MockJob(plusOneFailure, inputs = Set(twoPlusTwo))

    val status = executeJobsAndMapToStatuses(Set(plusOne))

    val alwaysExpected = Map(
      twoPlusTwo -> twoPlusTwoFailure,
      plusOne -> plusOneFailure)
    
    val expected0 = alwaysExpected + (two0Failed -> two0Failure)
    val expected1 = alwaysExpected + (two1Failed -> two1Failure)

    assert(status == expected0 || status == expected1)
  }
  
  withExecuter("execute() should work if some (late) sub-jobs fail") {

    val twoPlusTwo = MockJob(twoPlusTwoFailure, inputs = Set(two0, two1))

    val plusOne = MockJob(plusOneFailure, inputs = Set(twoPlusTwo))

    val status = executeJobsAndMapToStatuses(Set(plusOne))

    val expected = Map(
        two0 -> two0Success,
        two1 -> two1Success,
        twoPlusTwo -> twoPlusTwoFailure,
        plusOne -> plusOneFailure)

    assert(status === expected)
  }
}

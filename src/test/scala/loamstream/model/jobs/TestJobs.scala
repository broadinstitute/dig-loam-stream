package loamstream.model.jobs

import loamstream.model.jobs.LJob.SimpleSuccess
import loamstream.model.jobs.LJob.SimpleFailure

/**
 * @author clint
 * date: Jun 2, 2016
 */
trait TestJobs {
  protected val two0Success = SimpleSuccess("2(0)")
  protected val two1Success = SimpleSuccess("2(1)")

  protected val twoPlusTwoSuccess = SimpleSuccess("2 + 2")

  protected val plusOneSuccess = SimpleSuccess("(2 + 2) + 1")
  
  protected val two0Failure = SimpleFailure("2(0)")
  protected val two1Failure = SimpleFailure("2(1)")

  protected val twoPlusTwoFailure = SimpleFailure("2 + 2")

  protected val plusOneFailure = SimpleFailure("(2 + 2) + 1")

  protected val two0 = MockJob(two0Success)
  protected val two1 = MockJob(two1Success)

  protected val twoPlusTwo = MockJob(twoPlusTwoSuccess, inputs = Set(two0, two1))

  protected val plusOne = MockJob(plusOneSuccess, inputs = Set(twoPlusTwo))
  
  protected val two0Failed = MockJob(two0Failure)
  protected val two1Failed = MockJob(two1Failure)

  protected val twoPlusTwoFailed = MockJob(twoPlusTwoFailure, inputs = Set(two0Failed, two1Failed))

  protected val plusOneFailed = MockJob(plusOneFailure, inputs = Set(twoPlusTwoFailed))
}
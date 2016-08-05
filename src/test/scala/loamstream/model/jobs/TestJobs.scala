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

  protected val two0 = MockLJob(Set.empty, Set.empty, two0Success)
  protected val two1 = MockLJob(Set.empty, Set.empty,two1Success)

  protected val twoPlusTwo = MockLJob(Set(two0, two1), Set.empty, twoPlusTwoSuccess)

  protected val plusOne = MockLJob(Set(twoPlusTwo), Set.empty, plusOneSuccess)
  
  protected val two0Failed = MockLJob(Set.empty, Set.empty, two0Failure)
  protected val two1Failed = MockLJob(Set.empty, Set.empty, two1Failure)

  protected val twoPlusTwoFailed = MockLJob(Set(two0Failed, two1Failed), Set.empty, twoPlusTwoFailure)

  protected val plusOneFailed = MockLJob(Set(twoPlusTwoFailed), Set.empty, plusOneFailure)
}
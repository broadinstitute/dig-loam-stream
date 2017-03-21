package loamstream.model.jobs

/**
 * @author clint
 * date: Jun 2, 2016
 */
trait TestJobs {
  protected val two0Success: JobState = JobState.Succeeded
  protected val two1Success: JobState = JobState.Succeeded

  protected val twoPlusTwoSuccess: JobState = JobState.Succeeded

  protected val plusOneSuccess: JobState = JobState.Succeeded
  
  protected val two0Failure: JobState = JobState.Failed()
  protected val two1Failure: JobState = JobState.Failed()

  protected val twoPlusTwoFailure: JobState = JobState.Failed()

  protected val plusOneFailure: JobState = JobState.Failed()

  protected val two0 = MockJob(two0Success)
  protected val two1 = MockJob(two1Success)

  protected val twoPlusTwo = MockJob(twoPlusTwoSuccess, inputs = Set(two0, two1))

  protected val plusOne = MockJob(plusOneSuccess, inputs = Set(twoPlusTwo))
  
  protected val two0Failed = MockJob(two0Failure)
  protected val two1Failed = MockJob(two1Failure)

  protected val twoPlusTwoFailed = MockJob(twoPlusTwoFailure, inputs = Set(two0Failed, two1Failed))

  protected val plusOneFailed = MockJob(plusOneFailure, inputs = Set(twoPlusTwoFailed))
}

package loamstream.model.jobs

/**
 * @author clint
 * date: Jun 2, 2016
 */
trait TestJobs {
  protected val two0Success: JobResult = JobResult.Succeeded
  protected val two1Success: JobResult = JobResult.Succeeded

  protected val twoPlusTwoSuccess: JobResult = JobResult.Succeeded

  protected val plusOneSuccess: JobResult = JobResult.Succeeded
  
  protected val two0Failure: JobResult = JobResult.Failed()
  protected val two1Failure: JobResult = JobResult.Failed()

  protected val twoPlusTwoFailure: JobResult = JobResult.Failed()

  protected val plusOneFailure: JobResult = JobResult.Failed()

  protected val two0 = MockJob(two0Success)
  protected val two1 = MockJob(two1Success)

  protected val twoPlusTwo = MockJob(twoPlusTwoSuccess, inputs = Set(two0, two1))

  protected val plusOne = MockJob(plusOneSuccess, inputs = Set(twoPlusTwo))
  
  protected val two0Failed = MockJob(two0Failure)
  protected val two1Failed = MockJob(two1Failure)

  protected val twoPlusTwoFailed = MockJob(twoPlusTwoFailure, inputs = Set(two0Failed, two1Failed))

  protected val plusOneFailed = MockJob(plusOneFailure, inputs = Set(twoPlusTwoFailed))
}
